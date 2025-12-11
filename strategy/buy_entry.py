# strategy/buy_entry.py

import os
import time
import pandas as pd

from api import (
    get_current_ask_price,
    get_order_results_by_uuids,
    get_accounts,
)
from api.db_usstocks import get_current_last_price, get_current_bid_price, is_spread_too_wide
from manager.order_executor import execute_buy_orders
from strategy.casino_strategy import generate_buy_orders


BUY_LOG_COLUMNS = [
    "time",
    "market",
    "target_price",
    "buy_amount",
    "buy_units",
    "buy_type",
    "buy_uuid",
    "filled",
]


# ------------------------------------------------------------
# 공통 유틸
# ------------------------------------------------------------

def atomic_save(df: pd.DataFrame, path: str, retry: int = 5, delay: float = 0.5):
    """
    CSV 저장의 atomic 버전.
    - 파일 잠김(WinError 5)이면 delay 후 재시도
    - retry 횟수 초과 시 예외 발생
    """
    tmp = path + ".tmp"
    df.to_csv(tmp, index=False)

    for i in range(retry):
        try:
            os.replace(tmp, path)
            return  # 성공 시 종료
        except PermissionError as e:
            # Windows 파일 점유 문제 → 재시도
            if i < retry - 1:
                print(f"⚠️ [atomic_save] 파일 잠김 → 재시도 {i+1}/{retry} (대기 {delay}s) → {path}")
                time.sleep(delay)
                continue
            else:
                print(f"❌ [atomic_save] 재시도 실패 → 저장 불가")
                raise e
        except Exception as e:
            # 다른 예외는 그대로
            raise e



def load_setting_data() -> pd.DataFrame:
    """
    setting.csv 로드
    """
    print("[buy_entry.py] setting.csv 불러오는 중")
    return pd.read_csv("setting.csv")

# ------------------------------------------------------------
# 1) 1분 단위: 매수 주문 생성 플로우
# ------------------------------------------------------------

def _load_buy_log() -> pd.DataFrame:
    if not os.path.exists("buy_log.csv"):
        return pd.DataFrame(columns=BUY_LOG_COLUMNS)
    df = pd.read_csv("buy_log.csv", dtype={"buy_uuid": str})
    # 컬럼 보정
    for col in BUY_LOG_COLUMNS:
        if col not in df.columns:
            df[col] = "" if col in ["buy_uuid", "filled", "buy_type", "market", "time"] else 0
    return df


def _normalize_filled_column(df: pd.DataFrame) -> pd.DataFrame:
    if "filled" not in df.columns:
        df["filled"] = ""
    df["filled"] = df["filled"].fillna("").astype(str).str.strip()
    return df


def run_buy_generate_flow():
    """
    1분에 한 번 호출되는 매수 생성 메인 플로우.
    - setting.csv / buy_log.csv / 현재 보유를 기반으로
      generate_buy_orders()를 호출해 신규/보완 주문 생성
    """
    print("\n[buy_entry.py] ▶ 1분 단위 매수 생성 플로우 시작")

    setting_df = load_setting_data()
    buy_log_df = _load_buy_log()
    buy_log_df = _normalize_filled_column(buy_log_df)

    market_to_code = dict(zip(setting_df["market"], setting_df["market_code"]))

    # 📌 스프레드 방어 + 현재가 수집
    current_prices = {}
    for market in setting_df["market"].unique():
        market_code = market_to_code[market]

        # ① 스프레드 확인
        try:
            too_wide, pct, bid, ask = is_spread_too_wide(market, market_code)
        except Exception as e:
            print(f"⚠️ [buy_entry.py] {market} 스프레드 조회 실패 → 현재가 조회 스킵: {e}")
            continue

        if too_wide:
            print(
                f"🚫 [buy_entry.py] {market} 매수 생성 보류 — 스프레드 {pct:.2%} "
                f"(bid={bid}, ask={ask})"
            )
            # 스프레드 정상화 후 다음 루프에서 매수 가능
            continue

        # ② 스프레드 OK → 현재가 조회
        try:
            current_prices[market] = get_current_bid_price(
                market=market,
                market_code=market_code
            )
        except Exception as e:
            print(f"❌ [buy_entry.py] {market} 현재가 조회 실패: {e}")

    # 📌 current_prices가 비어 있으면 주문 생성할 필요 없음
    if not current_prices:
        print("⏸ [buy_entry.py] 스프레드 허용된 종목 없음 → generate_buy_orders 스킵")
        return

    # 실제 generate 호출
    print("[buy_entry.py] generate_buy_orders() 호출")
    updated_buy_log_df = generate_buy_orders(
        setting_df=setting_df[setting_df["market"].isin(current_prices.keys())],
        buy_log_df=buy_log_df,
        current_prices=current_prices,
        mode="normal",
    )

    # 실제 주문 실행
    try:
        updated_buy_log_df = execute_buy_orders(updated_buy_log_df)
        atomic_save(updated_buy_log_df, "buy_log.csv")
        print("[buy_entry.py] ✅ 모든 매수 주문 처리 완료 → buy_log.csv 저장")
    except Exception as e:
        print(f"🚨 [buy_entry.py] 매수 주문 실행 실패: {e}")
        import sys
        sys.exit(1)

    print("[buy_entry.py] ▶▶ 1분 단위 매수 생성 플로우 종료")



# ------------------------------------------------------------
# 2) 초 단위: 매수 체결 감지 (wait → done)
# ------------------------------------------------------------

def detect_filled_buy_orders():
    """
    초 단위로 호출.
    - buy_log.csv에서 filled in ["", "wait", "update"] 이고 buy_uuid 존재하는 주문만 조회
    - get_order_results_by_uuids()로 상태 확인
    - 상태 변경 사항을 buy_log.csv에 반영
    - 특히 'wait/""/update → done' 으로 변경된 주문을 리스트로 반환
      → 매도 로직에서 바로 활용할 수 있음

    ❗ 중요:
    - API 응답에 없는 uuid(missing_uuids)는 "삭제하지 않는다".
      → race condition으로 인한 정상 체결 주문 삭제를 방지.
    - uuid는 항상 문자열로 정규화해서 비교한다.
    """
    print("\n[buy_entry.py] ▶ 매수 체결 감지 플로우 시작")

    if not os.path.exists("buy_log.csv"):
        print("[buy_entry.py] buy_log.csv 없음 → 감지할 주문 없음")
        return []

    df = pd.read_csv("buy_log.csv", dtype={"buy_uuid": str})
    if df.empty:
        print("[buy_entry.py] buy_log.csv 비어 있음 → 감지할 주문 없음")
        return []

    # filled 문자열 정규화
    df = _normalize_filled_column(df)

    # uuid 문자열 컬럼 추가 (float → str, .0 제거 등)
    df["buy_uuid_str"] = (
        df["buy_uuid"]
        .fillna("")
        .astype(str)
        .str.strip()
        .str.replace(".0", "", regex=False)
    )

    # 대기 중인 주문만 대상
    pending_mask = df["buy_uuid_str"].ne("") & df["filled"].isin(["", "wait", "update"])
    pending_df = df[pending_mask].copy()

    if pending_df.empty:
        print("[buy_entry.py] 대기 중인 매수 주문 없음")
        # 그래도 buy_log 정규화 저장은 해두자
        df["buy_uuid"] = (
            df["buy_uuid"]
            .fillna("")
            .astype(str)
            .str.strip()
            .str.replace(".0", "", regex=False)
        )
        if "buy_uuid_str" in df.columns:
            df = df.drop(columns=["buy_uuid_str"])
        atomic_save(df, "buy_log.csv")
        print("[buy_entry.py] buy_log.csv 상태 업데이트 완료")
        print("[buy_entry.py] ▶ 매수 체결 이벤트 수: 0")
        return []

    filled_events = []  # 매수 체결 이벤트 리스트

    # market별로 uuid 조회
    markets = pending_df["market"].unique()
    for market in markets:
        market_pending = pending_df[pending_df["market"] == market].copy()
        uuid_list = market_pending["buy_uuid_str"].tolist()

        # 1차 상태 조회
        try:
            status_map = get_order_results_by_uuids(uuid_list, market)
        except Exception as e:
            print(f"❌ [buy_entry.py] 주문 상태 조회 중 오류 발생 ({market}): {e}")
            continue

        # 각 row에 대해 상태 반영
        for idx, row in df.iterrows():
            if row.get("market") != market:
                continue

            uuid = row.get("buy_uuid_str", "")
            if not uuid:
                continue

            if row.get("filled", "") not in ["", "wait", "update"]:
                # 이미 done/cancel 등으로 확정된 주문
                continue

            state = status_map.get(uuid)
            if state is None:
                # 응답에서 빠진 uuid는 삭제/변경하지 않는다 (race condition 방지)
                continue

            state = str(state).lower()

            # 1) 체결 완료
            if state == "done":
                if df.at[idx, "filled"] != "done":
                    df.at[idx, "filled"] = "done"
                    print(f"✅ [buy_entry.py] {market} 매수 주문 {uuid} → done 반영")

                    filled_events.append({
                        "market": row["market"],
                        "buy_uuid": uuid,
                        "buy_type": row.get("buy_type", ""),
                        "buy_amount": float(row.get("buy_amount", 0) or 0),
                        "buy_units": float(row.get("buy_units", 0) or 0),
                        "target_price": float(row.get("target_price", 0) or 0),
                        "row_index": idx,
                    })

            # 2) 취소된 주문 → 딜레이 후 한 번 더 재확인
            elif state == "cancel":
                print(f"⚠️ [buy_entry.py] {market} 주문 {uuid} → cancel 응답(임시)")

                # API가 cancel을 너무 빨리 줄 수 있으므로, 짧게 대기 후 재조회
                time.sleep(1.0)

                try:
                    recheck_map = get_order_results_by_uuids([uuid], market)
                except Exception as e:
                    print(f"⚠️ [buy_entry.py] {market} 주문 {uuid} 재조회 실패 → {e}")
                    # 재조회 실패 시 일단 cancel로 두고, 다음 루프에서 다시 기회를 준다
                    df.at[idx, "filled"] = "cancel"
                    continue

                re_state = str(recheck_map.get(uuid, "cancel")).lower()
                print(f"[buy_entry.py] {market} 주문 {uuid} → 재확인 state={re_state}")

                if re_state == "done":
                    print(f"🔥 [buy_entry.py] {market} 주문 {uuid} → 재확인 결과 실제 체결 → done 처리")
                    df.at[idx, "filled"] = "done"

                    filled_events.append({
                        "market": row["market"],
                        "buy_uuid": uuid,
                        "buy_type": row.get("buy_type", ""),
                        "buy_amount": float(row.get("buy_amount", 0) or 0),
                        "buy_units": float(row.get("buy_units", 0) or 0),
                        "target_price": float(row.get("target_price", 0) or 0),
                        "row_index": idx,
                    })
                else:
                    print(f"⚠️ [buy_entry.py] {market} 주문 {uuid} → 최종 cancel 처리")
                    df.at[idx, "filled"] = "cancel"

            # 3) 그 외(wait 등)는 그대로 유지

    # 보조 컬럼 정리 후 저장
    if "buy_uuid_str" in df.columns:
        df = df.drop(columns=["buy_uuid_str"])
    df["buy_uuid"] = (
        df["buy_uuid"]
        .fillna("")
        .astype(str)
        .str.strip()
        .str.replace(".0", "", regex=False)
    )
    atomic_save(df, "buy_log.csv")
    print("[buy_entry.py] buy_log.csv 상태 업데이트 완료")
    print(f"[buy_entry.py] ▶ 매수 체결 이벤트 수: {len(filled_events)}")

    return filled_events



# ------------------------------------------------------------
# 3) 전량 매도 후 initial 재진입(초 단위)
# ------------------------------------------------------------

def process_sold_out_markets_for_initial(setting_df: pd.DataFrame):
    """
    '전량 매도 후 다시 1U initial 진입'을 초 단위로 처리하는 함수.

    - 현재 보유가 0인 종목 중에서,
      * buy_log에 '진행 중인(initial, filled in ["", "wait", "update"])' 주문이 없고

      이런 종목에만 1U initial 지정가 매수 주문을 생성해서 실행한다.

    - 더 이상 기존 buy_log를 삭제하지 않는다.
      (전량 매도에 따른 buy_log/sell_log 정리는
       sell_entry.clean_buy_and_sell_logs_after_full_sell 에서만 담당)
    """
    if setting_df is None or setting_df.empty:
        print("[buy_entry.py] process_sold_out_markets_for_initial: setting_df 비어있음 → 스킵")
        return

    # 1) 현재 보유 종목 조회
    accounts = get_accounts()
    current_holdings = set(
        [m for m, pos in accounts.items() if float(pos.get("balance", 0) or 0) > 0]
    )

    # 2) buy_log 로드
    buy_log_df = _load_buy_log()
    buy_log_df = _normalize_filled_column(buy_log_df)

    if not buy_log_df.empty:
        buy_log_df["buy_uuid_str"] = buy_log_df["buy_uuid"].fillna("").astype(str).str.strip()
    else:
        buy_log_df["buy_uuid_str"] = []

    setting_markets = list(setting_df["market"])
    need_initial_buy = [m for m in setting_markets if m not in current_holdings]

    for market in need_initial_buy:
        print(f"🧹 [buy_entry.py] [{market}] 전량 매도 상태 감지 → initial 진입 여부 체크")

        market_logs = buy_log_df[buy_log_df["market"] == market].copy()

        # 이미 pending initial 주문이 있으면 신규 생성 X
        if not market_logs.empty:
            has_pending_initial = (
                    (market_logs["buy_type"] == "initial")
                    & market_logs["buy_uuid_str"].ne("")
                    & market_logs["filled"].isin(["", "wait", "update"])
            ).any()

            if has_pending_initial:
                print(f"⏸ [buy_entry.py] [{market}] pending initial 주문 존재 → 신규 생성 스킵")
                continue

        # 여기까지 왔으면:
        # - 현재 보유 0
        # - pending initial 없음
        # → 새 initial 1U 주문 생성 가능
        market_code = setting_df.loc[setting_df["market"] == market, "market_code"].iloc[0]

        try:
            too_wide, pct, bid, ask = is_spread_too_wide(market, market_code)
        except Exception as e:
            print(f"⚠️ [buy_entry.py] [{market}] 스프레드 조회 실패 → initial 생성 보류: {e}")
            continue

        if too_wide:
            print(
                f"🚫 [buy_entry.py] [{market}] initial 생성 보류 — 스프레드 {pct:.2%} "
                f"(bid={bid}, ask={ask})"
            )
            # 스프레드가 정상화되면 다음 루프에서 다시 initial 생성 조건을 통과하게 됨
            continue

        try:
            current_price = get_current_ask_price(market=market, market_code=market_code)
        except Exception as e:
            print(f"❌ [buy_entry.py] [{market}] 현재가 조회 실패 → initial 생성 스킵: {e}")
            continue

        current_prices = {market: current_price}

        print(f"🧽 [buy_entry.py] [{market}] initial_only 모드로 1U 매수 주문 생성")
        new_buy_logs = generate_buy_orders(
            setting_df=setting_df[setting_df["market"] == market],
            buy_log_df=market_logs,  # 기존 로그는 참고만
            current_prices=current_prices,
            mode="initial_only",
        )

        # 주문 실행
        try:
            new_buy_logs = execute_buy_orders(new_buy_logs)
        except Exception as e:
            print(f"🚨 [buy_entry.py] [{market}] initial 주문 실행 실패: {e}")
            continue

        # full buy_log에 append
        combined = pd.concat([buy_log_df, new_buy_logs], ignore_index=True)

        if "buy_uuid_str" in combined.columns:
            combined = combined.drop(columns=["buy_uuid_str"])
        combined["buy_uuid"] = (
            combined["buy_uuid"]
            .fillna("")
            .astype(str)
            .str.strip()
            .str.replace(".0", "", regex=False)
        )
        atomic_save(combined, "buy_log.csv")
        buy_log_df = combined  # 메모리 상에서도 최신 상태로 갱신

        print(f"✅ [buy_entry.py] [{market}] initial 1U 매수 주문 생성 및 접수 완료")
