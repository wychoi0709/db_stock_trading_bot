# manager/order_executor.py

import pandas as pd
from api import send_order, cancel_and_new_order
from utils.kis_utils import normalize_uuid

# manager/order_executor.py
# 반드시 이 파일 안에서 check_market_closed를 아래로 교체해라

DB_MARKET_CLOSED_CODES = {
    "2611",   # 장시작 전 또는 장마감
    "3563",   # 정규매매장 종료, 시간외 주문 필요
    "3589",   # 장개시 전
    "3590",   # 장마감 후
    "8819",   # 주문가능 시간이 아닙니다.
    "3107",   # 휴장일임으로 처리가 불가능합니다.
}

DB_MARKET_CLOSED_KEYWORDS = [
    "장마감",
    "장 마감",
    "정규매매장이 종료",
    "장개시 전",
    "시간외",
    "주문 불가",
    "NXT거래",
    "서비스 일시정지",
    "주문가능",
    "시각이 아닙니다",
    "휴장일"
]

def detect_market_closed_from_exception(e: Exception):
    """
    Exception → 문자열 msg 로 변환하여
    DB 폐장 코드 / 키워드 / MARKET_CLOSED 키워드
    모두 감지하여 RuntimeError("MARKET_CLOSED") 발생시킴.
    """
    msg = str(e)

    # 0) 기존 MARKET_CLOSED 메시지 전파
    if "MARKET_CLOSED" in msg:
        raise RuntimeError("MARKET_CLOSED")

    # 1) DB 폐장 코드 감지
    for code in DB_MARKET_CLOSED_CODES:
        if code in msg:
            print(f"⛔ [detect] DB 폐장 코드 감지({code}) → MARKET_CLOSED 전파")
            raise RuntimeError("MARKET_CLOSED")

    # 2) DB 폐장 키워드 감지
    for kw in DB_MARKET_CLOSED_KEYWORDS:
        if kw in msg:
            print(f"⛔ [detect] DB 폐장 키워드 감지({kw}) → MARKET_CLOSED 전파")
            raise RuntimeError("MARKET_CLOSED")



def check_market_closed(response: dict):
    """
    KIS + DB증권 공통 장마감 감지
    주문 응답(response)에서 시장 폐장 상태를 감지하면
    RuntimeError("MARKET_CLOSED: ...") 발생시킴.
    """

    raw = response.get("raw", {}) if isinstance(response, dict) else {}

    # DB + KIS 공통 코드
    msg_cd = str(raw.get("rsp_cd") or raw.get("msg_cd") or "").strip()
    msg1   = str(raw.get("rsp_msg") or raw.get("msg1") or "").strip()
    msgall = f"{msg_cd} {msg1}"

    # -----------------------------
    # 1) 코드 기반 (DB)
    # -----------------------------
    if msg_cd in DB_MARKET_CLOSED_CODES:
        raise RuntimeError(f"MARKET_CLOSED: {msgall}")

    # -----------------------------
    # 2) 메시지 기반 (키워드 포함)
    # -----------------------------
    for kw in DB_MARKET_CLOSED_KEYWORDS:
        if kw in msg1:
            raise RuntimeError(f"MARKET_CLOSED: {msgall}")

    # -----------------------------
    # 3) KIS 형식 메시지 generic 대응
    # -----------------------------
    low = msg1.lower()
    if "market" in low and "close" in low:
        raise RuntimeError(f"MARKET_CLOSED: {msgall}")



def execute_buy_orders(buy_log_df: pd.DataFrame) -> pd.DataFrame:
    print("[order_executor.py] 매수 주문 실행 시작")
    all_success = True

    for idx, row in buy_log_df.iterrows():
        filled = str(row.get("filled", "")).strip()
        uuid = row.get("buy_uuid", None)

        if filled == "done":
            continue

        market = row["market"]
        amount = float(row["buy_amount"])
        price = float(row["target_price"])

        # 정수 주식 단위 계산
        volume = int(amount // price)
        if volume <= 0:
            print(f"⚠️ {market}: 현재가 {price:.2f}$ → {amount}$으로 매수 불가 (스킵)")
            continue

        # 정정 주문
        if filled == "update" and pd.notna(uuid):
            print(f"🔁 정정 매수 주문: {market}, uuid={uuid}, {volume}주 @ {price:.2f}$")
            try:
                response = cancel_and_new_order(
                    prev_order_uuid=uuid,
                    market=market,
                    price=price,
                    quantity=volume,
                    side="BUY"
                )

                check_market_closed(response)

                new_uuid = normalize_uuid(response.get("new_order_uuid", ""))
                if new_uuid:
                    buy_log_df.at[idx, "buy_uuid"] = str(new_uuid)
                    buy_log_df.at[idx, "filled"] = "wait"
                else:
                    raise ValueError("정정 매수 주문 new_uuid 없음")
            except Exception as e:
                detect_market_closed_from_exception(e)

                print(f"❌ 정정 매수 주문 실패: {e}")
                all_success = False

        # 신규 주문
        elif filled == "update" and pd.isna(uuid):
            print(f"🆕 신규 매수 주문: {market}, {volume}주 @ {price:.2f}$")
            try:
                buy_type = row.get("buy_type", "")

                # -----------------------------
                # INITIAL → MARKET 주문 시도
                # 프리장/애프터에서 실패하면 LIMIT로 fallback
                # -----------------------------
                if buy_type == "initial":
                    try:
                        print(f"⚡ INITIAL 주문 → 우선 시장가(MARKET)로 시도: {market}")
                        response = send_order(
                            market=market,
                            side="BUY",
                            ord_type="market",  # 우선 시장가로 시도
                            unit_price=None,  # 시장가는 가격 없음
                            volume=volume
                        )

                        # 응답에서 status 실패 시 예외 처리
                        if str(response.get("rt_cd", "0")) != "0":
                            raise Exception(f"시장가 주문 실패: {response}")

                    except Exception as e:
                        print(f"⚠️ 시장가 주문 실패 → 지정가로 재시도: {e}")
                        # fallback → 지정가 주문
                        response = send_order(
                            market=market,
                            side="BUY",
                            ord_type="limit",
                            unit_price=price,
                            volume=volume
                        )
                # -----------------------------
                # SMALL / LARGE → 기존처럼 LIMIT
                # -----------------------------
                else:
                    response = send_order(
                        market=market,
                        side="BUY",
                        ord_type="limit",  # 필요 시 buy_type=="initial"이면 "market" 등으로 분기 가능
                        unit_price=price,
                        volume=volume
                    )

                check_market_closed(response)

                new_uuid = normalize_uuid(response.get("uuid", ""))
                if new_uuid:
                    buy_log_df.at[idx, "buy_uuid"] = str(new_uuid)
                    buy_log_df.at[idx, "filled"] = "wait"
                else:
                    raise ValueError("신규 매수 주문 uuid 없음")
            except Exception as e:
                detect_market_closed_from_exception(e)

                print(f"❌ 신규 매수 주문 실패: {e}")
                all_success = False

    print("[order_executor.py] 매수 주문 실행 완료")

    if not all_success:
        raise RuntimeError("일부 매수 주문 실패")

    return buy_log_df


def execute_sell_orders(sell_log_df: pd.DataFrame, holdings: dict) -> pd.DataFrame:
    print("[order_executor.py] 매도 주문 실행 시작")
    all_success = True

    for idx, row in sell_log_df.iterrows():
        filled = str(row.get("filled", "")).strip()
        uuid = row.get("sell_uuid", None)

        if filled == "done":
            continue  # 이미 완료된 주문은 스킵

        market = row["market"]
        price = float(row["target_sell_price"])

        # 보유 수량 확인 (정수 주식 단위)
        volume = int(float(holdings.get(market, {}).get("balance", 0)))
        if volume <= 0:
            print(f"⚠️ {market} 매도할 수량이 0 → 스킵 (filled=done 처리)")
            sell_log_df.at[idx, "filled"] = "done"
            continue

        # 정정 매도 주문
        if filled == "update" and pd.notna(uuid):
            print(f"🔁 정정 매도 주문: {market}, uuid={uuid}, {volume}주 @ {price:.2f}$")
            try:
                response = cancel_and_new_order(
                    prev_order_uuid=uuid,
                    market=market,
                    price=price,
                    quantity=volume,
                    side="SELL"
                )

                check_market_closed(response)

                new_uuid = normalize_uuid(response.get("new_order_uuid", ""))
                if new_uuid:
                    sell_log_df.at[idx, "sell_uuid"] = str(new_uuid)
                    sell_log_df.at[idx, "filled"] = "wait"
                else:
                    raise ValueError("정정 매도 주문 new_uuid 없음")

            except Exception as e:
                # -----------------------------
                # ① 실패 원인 분석 (8819 여부 확인)
                # -----------------------------
                try:
                    err = e.args[0] if e.args else ""
                except:
                    err = str(e)

                # 정정취소 불가(rsp_cd=8819) → 신규 매도 대체
                if ("8819" in err) or ("정정취소" in err):
                    print(f"⚠️ {market} 정정 취소 불가 → 신규 매도 주문으로 대체 진행")

                    try:
                        # -----------------------------
                        # ② 신규 매도 주문 실행
                        # -----------------------------
                        response = send_order(
                            market=market,
                            side="ask",
                            ord_type="limit",
                            unit_price=price,
                            volume=volume,
                            amount_krw=None
                        )

                        new_uuid = response.get("uuid", "")

                        if new_uuid:
                            print(f"🟢 신규 매도 주문 성공 → uuid={new_uuid}")
                            sell_log_df.at[idx, "sell_uuid"] = new_uuid
                            sell_log_df.at[idx, "filled"] = "wait"
                        else:
                            raise ValueError("❌ 신규 매도 uuid 없음 (정정 실패 후 대체 주문 실패)")

                    except Exception as new_e:
                        print(f"❌ 신규 매도 주문 실패(대체 실패): {new_e}")
                        detect_market_closed_from_exception(new_e)

                else:
                    print(f"❌ 정정 매도 주문 실패: {e}")
                    all_success = False

                    # 기존 예외 처리 유지
                    detect_market_closed_from_exception(e)



        # 신규 매도 주문
        elif filled == "update" and pd.isna(uuid):
            print(f"🆕 신규 매도 주문: {market}, {volume}주 @ {price:.2f}$")
            try:
                response = send_order(
                    market=market,
                    side="SELL",
                    ord_type="limit",
                    unit_price=price,
                    volume=volume
                )

                check_market_closed(response)

                new_uuid = normalize_uuid(response.get("uuid", ""))
                if new_uuid:
                    sell_log_df.at[idx, "sell_uuid"] = str(new_uuid)
                    sell_log_df.at[idx, "filled"] = "wait"
                else:
                    raise ValueError("신규 매도 주문 uuid 없음")
            except Exception as e:
                detect_market_closed_from_exception(e)

                print(f"❌ 신규 매도 주문 실패: {e}")
                all_success = False

    print("[order_executor.py] 매도 주문 실행 완료")

    if not all_success:
        raise RuntimeError("일부 매도 주문 실패")

    return sell_log_df
