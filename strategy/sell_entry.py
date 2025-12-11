# strategy/sell_entry.py

import os
from datetime import time

import pandas as pd

from api import get_accounts, get_current_ask_price, get_order_results_by_uuids
from strategy.casino_strategy import generate_sell_orders
from manager.order_executor import execute_sell_orders
from api import cancel_orders_by_uuids


SELL_LOG_COLUMNS = [
    "market",
    "avg_buy_price",
    "quantity",
    "target_sell_price",
    "sell_uuid",
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



def load_setting_data():
    print("[sell_entry.py] setting.csv 불러오는 중")
    return pd.read_csv("setting.csv")


# ------------------------------------------------------------
# 전량 매도 후 buy_log/sell_log 정리
# ------------------------------------------------------------

def clean_buy_and_sell_logs_after_full_sell(market: str):
    print(f"[DEBUG][CLEAN_FULL_SELL] 실행됨 → market={market}")

    # 1) buy_log에서 해당 코인에 걸린 미체결 uuid → cancel 요청 후 삭제
    if os.path.exists("buy_log.csv"):
        buy_df = pd.read_csv("buy_log.csv", dtype={"buy_uuid": str})
        print(f"[DEBUG][CLEAN_FULL_SELL] buy_log 로드 결과 rows={len(buy_df)}")

        market_logs = buy_df[buy_df["market"] == market].copy()
        other_logs = buy_df[buy_df["market"] != market].copy()

        uuids = (
            market_logs["buy_uuid"]
            .fillna("")
            .astype(str)
            .str.strip()
        )
        uuids = [u for u in uuids.tolist() if u]

        if uuids:
            print(f"🗑️ [{market}] 미체결 buy 주문 취소 요청 → {uuids}")
            try:
                cancel_orders_by_uuids(uuids, market)
            except Exception as e:
                print(f"⚠️ [{market}] buy uuid 취소 실패 → {e}")

        # 해당 market의 buy_log row 삭제
        atomic_save(other_logs, "buy_log.csv")
        print(f"[DEBUG][CLEAN_FULL_SELL] buy_log에서 [{market}] 관련 로그 삭제 완료")

    # 2) sell_log에서 해당 market 삭제
    if os.path.exists("sell_log.csv"):
        sell_df = pd.read_csv("sell_log.csv", dtype={"sell_uuid": str})
        before_rows = len(sell_df)
        sell_df = sell_df[sell_df["market"] != market]
        after_rows = len(sell_df)
        atomic_save(sell_df, "sell_log.csv")
        print(f"[DEBUG][CLEAN_FULL_SELL] sell_log에서 [{market}] 관련 로그 {before_rows - after_rows}건 삭제")

    print(f"🧽 [{market}] 전량 매도 cleanup 완료")


# ------------------------------------------------------------
# 매도 전용 보유 현황 조회
# ------------------------------------------------------------

def get_current_holdings_for_sell(setting_df):
    """
    매도 전용 보유 포지션 조회.
    - side == LONG 인 것만 대상으로 함.
    """
    print("[sell_entry.py] 현재 보유 자산 조회 중")
    accounts = get_accounts()
    holdings = {}

    for symbol, pos in accounts.items():
        if pos.get("side", "LONG") != "LONG":
            continue

        try:
            balance = float(pos.get("balance", 0) or 0)
            if balance <= 0:
                continue
            # ⭐ setting에서 market_code 가져오기
            market_code = setting_df.loc[
                setting_df["market"] == symbol, "market_code"
            ].iloc[0]

            # ⭐ 현재가 조회
            current_price = get_current_ask_price(
                market=symbol,
                market_code=market_code
            )
        except Exception as e:
            print(f"❌ [sell_entry.py] {symbol} 현재가 조회 실패: {e}")
            continue

        holdings[symbol] = {
            "balance": balance,
            "locked": float(pos.get("locked", 0) or 0),
            "avg_price": float(pos.get("avg_buy_price", 0) or 0),
            "current_price": current_price,
            "side": pos.get("side", "LONG"),
            "liquidation_price": pos.get("liquidation_price"),
            "leverage": pos.get("leverage", 1),
        }

    print(f"[sell_entry.py] 현재 LONG 포지션 수: {len(holdings)}개")
    return holdings


# ------------------------------------------------------------
# sell_log 상태 업데이트 + 전량 매도 clean
# ------------------------------------------------------------

def _load_sell_log() -> pd.DataFrame:
    if not os.path.exists("sell_log.csv"):
        return pd.DataFrame(columns=SELL_LOG_COLUMNS)
    df = pd.read_csv("sell_log.csv", dtype={"sell_uuid": str})
    for col in SELL_LOG_COLUMNS:
        if col not in df.columns:
            df[col] = "" if col in ["market", "sell_uuid", "filled"] else 0
    return df


def update_sell_log_status_by_uuid(sell_log_df: pd.DataFrame) -> pd.DataFrame:
    """
    기존 매도 주문의 상태를 정리 (done/cancel 제거 등) +
    포지션이 0이 된 종목에 대해서는 전량 매도 clean까지 수행.
    """
    print("[sell_entry.py] sell_log.csv 주문 상태 확인 및 정리 중...")

    if sell_log_df is None or sell_log_df.empty:
        print("[sell_entry.py] 매도 로그 없음")
        return sell_log_df

    # 문자열 정규화
    sell_log_df["filled"] = sell_log_df["filled"].fillna("").astype(str).str.strip()
    sell_log_df["sell_uuid_str"] = (
        sell_log_df["sell_uuid"].fillna("").astype(str).str.strip()
    )

    # pending 주문만 상태 조회
    pending_df = sell_log_df[
        sell_log_df["sell_uuid_str"].ne("")
        & sell_log_df["filled"].isin(["", "wait", "update"])
    ].copy()

    if pending_df.empty:
        print("[sell_entry.py] 확인할 매도 주문이 없습니다.")
        # 그래도 포지션 0인 종목이 있으면 clean 해주기 위해 아래 포지션 체크는 수행
        markets_to_check = sell_log_df["market"].unique()
    else:
        markets_to_check = pending_df["market"].unique()

    indices_to_drop = []
    changed = False

    # 1) 상태 조회 및 done/cancel 정리
    for market in markets_to_check:
        market_pending = pending_df[pending_df["market"] == market].copy()
        uuid_list = market_pending["sell_uuid_str"].tolist()

        status_map = {}
        if uuid_list:
            try:
                status_map = get_order_results_by_uuids(uuid_list, market)
            except Exception as e:
                print(f"❌ [sell_entry.py] 주문 상태 조회 중 오류 발생 ({market}): {e}")

        for idx, row in sell_log_df.iterrows():
            if row["market"] != market:
                continue

            uuid = row.get("sell_uuid_str", "")
            if not uuid:
                continue

            state = status_map.get(uuid)
            if state is None:
                # 응답에서 빠진 uuid는 삭제하지 않고 유지
                continue

            # 체결 완료
            if state == "done":
                print(f"[DEBUG][SELL_STATUS] {market} 주문 {uuid} → done 감지됨")
                print(f"✅ [sell_entry.py] {market} 주문 체결 완료 → 로그에서 제거")
                indices_to_drop.append(idx)
                changed = True

            # 취소
            elif state == "cancel":
                print(f"⚠️ [sell_entry.py] {market} 주문 {uuid} → cancel 감지됨 → 로그에서 제거")
                indices_to_drop.append(idx)
                changed = True

            # 그 외(wait 등)는 유지

    if indices_to_drop:
        sell_log_df.drop(index=indices_to_drop, inplace=True)
        sell_log_df.reset_index(drop=True, inplace=True)
        print(f"[sell_entry.py] 완료/취소된 주문 {len(indices_to_drop)}건 삭제 완료")

    # 보조 컬럼 제거 전, 파일 저장은 나중에 한 번만
    # 2) 포지션 0인 종목에 대해 전량 매도 clean 수행
    accounts = get_accounts()

    for market in markets_to_check:
        pos_info = accounts.get(market, {})
        balance = float(pos_info.get("balance", 0) or 0)
        locked = float(pos_info.get("locked", 0) or 0)
        total_pos = balance + locked

        if total_pos <= 0.000001:
            print(f"[DEBUG][SELL_STATUS] {market} 포지션=0 → 전량 매도 판단! clean_buy_and_sell_logs_after_full_sell 실행")
            clean_buy_and_sell_logs_after_full_sell(market)

    if "sell_uuid_str" in sell_log_df.columns:
        sell_log_df = sell_log_df.drop(columns=["sell_uuid_str"])

    if changed:
        atomic_save(sell_log_df, "sell_log.csv")
        print("[sell_entry.py] 상태 변경 내용 저장 완료")

    return sell_log_df


# ------------------------------------------------------------
# 주기적 매도 상태 체크
# ------------------------------------------------------------

def periodic_sell_status_check():
    print("\n[sell_entry.py] ▶ 주기적 매도 주문 상태 체크 시작")

    try:
        sell_log_df = _load_sell_log()
    except Exception as e:
        print(f"[sell_entry.py] sell_log.csv 읽기 실패: {e}")
        return

    sell_log_df = update_sell_log_status_by_uuid(sell_log_df)

    # ============================
    # 2) done 상태 매도 로그 삭제
    # ============================
    sell_log_df = sell_log_df[
        sell_log_df["filled"].astype(str).str.strip() != "done"
        ].reset_index(drop=True)
    atomic_save(sell_log_df, "sell_log.csv")

    setting_df = load_setting_data()
    holdings = get_current_holdings_for_sell(setting_df)

    # 1) 보유 중인데 매도 주문이 없는 경우 → 신규 생성
    for market, pos in holdings.items():
        existing = sell_log_df[sell_log_df["market"] == market]

        has_pending = (
            not existing.empty
            and existing["filled"].fillna("").astype(str).str.strip().isin(
                ["", "wait", "update"]
            ).any()
        )

        if has_pending:
            continue

        print(f"⚠️ [sell_entry] {market} 보유 중인데 기존 매도 없음 → 신규 생성!")

        sub_setting = setting_df[setting_df["market"] == market]
        if sub_setting.empty:
            print(f"❌ [sell_entry] setting.csv에 {market} 설정 없음 → 매도 불가")
            continue

        new_sell_df = generate_sell_orders(sub_setting, {market: pos}, sell_log_df)

        new_sell_df = execute_sell_orders(new_sell_df, {market: pos})

        sell_log_df = pd.concat(
            [sell_log_df[sell_log_df["market"] != market], new_sell_df],
            ignore_index=True,
        )

        atomic_save(sell_log_df, "sell_log.csv")
        print(f"✅ [sell_entry] {market} 신규 매도 주문 생성 완료")

    # 2) ⭐ 보유 수량 변경 감지 → 기존 매도 주문 취소 후 새로 생성
    for market, pos in holdings.items():
        balance = round(float(pos.get("balance", 0) or 0), 8)
        locked = round(float(pos.get("locked", 0) or 0), 8)
        total_qty = balance + locked

        market_log = sell_log_df[sell_log_df["market"] == market]

        if not market_log.empty:
            existing_qty = round(float(market_log.iloc[0]["quantity"]), 8)

            if abs(existing_qty - total_qty) > 1e-8:
                print(f"⚠️ [sell_entry] {market} 보유수량 변경 감지! "
                      f"기존={existing_qty}, 현재={total_qty}")

                uuids = market_log["sell_uuid"].dropna().tolist()
                if uuids:
                    print(f"🗑️ [sell_entry] 기존 매도 주문 취소 요청 → {uuids}")
                    try:
                        cancel_orders_by_uuids(uuids, market)
                    except Exception as e:
                        print(f"⚠️ {market} 기존 매도 취소 실패: {e}")

                sell_log_df = sell_log_df[sell_log_df["market"] != market]

                sub_setting = setting_df[setting_df["market"] == market]
                if sub_setting.empty:
                    print(f"❌ [sell_entry] 설정 없음 → 매도 주문 생성 스킵")
                    continue

                new_sell_df = generate_sell_orders(sub_setting, {market: pos}, sell_log_df)
                new_sell_df = execute_sell_orders(new_sell_df, {market: pos})

                sell_log_df = pd.concat([sell_log_df, new_sell_df], ignore_index=True)
                atomic_save(sell_log_df, "sell_log.csv")

                print(f"✅ [sell_entry] {market} 보유수량 변경 반영 → 신규 매도 주문 생성 완료")

    print("[sell_entry.py] ▶ 주기적 매도 주문 상태 체크 종료")



# ------------------------------------------------------------
# 매수 체결 이벤트 기반 즉시 매도
# ------------------------------------------------------------

def immediate_sell_for_filled_buys(setting_df: pd.DataFrame, filled_events: list):
    """
    매수 체결 이벤트가 발생했을 때 '바로' 호출되는 매도 로직.
    - filled_events: detect_filled_buy_orders() 결과 리스트
    - 현재 보유 기준으로 전량 매도 주문 생성/정정 후 바로 실행
    """
    if not filled_events:
        return

    print("\n[sell_entry.py] ▶ 매수 체결 이벤트 기반 즉시 매도 플로우 시작")

    holdings = get_current_holdings_for_sell(setting_df)
    if not holdings:
        print("[sell_entry.py] 보유 포지션 없음 → 매도 주문 스킵")
        return

    try:
        sell_log_df = _load_sell_log()
    except Exception:
        sell_log_df = pd.DataFrame(columns=SELL_LOG_COLUMNS)

    # 기존 매도 주문 상태 정리
    sell_log_df = update_sell_log_status_by_uuid(sell_log_df)

    # generate_sell_orders() 로 새 타겟 매도 주문 생성/정정
    updated_sell_log_df = generate_sell_orders(setting_df, holdings, sell_log_df)

    # 실제 매도 주문 실행
    try:
        updated_sell_log_df = execute_sell_orders(updated_sell_log_df, holdings)
        atomic_save(updated_sell_log_df, "sell_log.csv")
        print("[sell_entry.py] ✅ 매도 주문 실행 및 sell_log.csv 저장 완료")
    except Exception as e:
        msg = str(e)
        if "MARKET_CLOSED" in msg:
            print("⛔ [sell_entry] MARKET_CLOSED 감지 → entry.py로 전파")
            raise
        print(f"🚨 [sell_entry.py] 매도 주문 실행 실패: {e}")
        import sys
        sys.exit(1)

    print("[sell_entry.py] ▶ 매수 체결 이벤트 기반 즉시 매도 플로우 종료")
