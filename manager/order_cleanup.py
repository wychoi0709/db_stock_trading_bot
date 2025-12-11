# manager/order_cleanup.py

import pandas as pd
from api.db_usstocks import (
    cancel_orders_by_uuids,
    get_all_open_buy_orders,
)
from strategy.buy_entry import load_setting_data


def cleanup_untracked_buy_orders():
    """
    setting.csv 대상 종목에 대해,
    buy_log.csv도 sell_log.csv도 없는 실제 미체결 매수 주문을 모두 취소한다.
    - 매초 entry.py 루프에서 실행됨
    """
    print("[cleanup] ▶ buy_log & sell_log 기준 외부 주문 검사 시작")

    # ======================================================
    # 1) setting.csv – 거래 대상 시장 리스트
    # ======================================================
    setting_df = load_setting_data()
    markets = setting_df["market"].unique().tolist()

    # ======================================================
    # 2) buy_log.csv 로드
    # ======================================================
    try:
        buy_df = pd.read_csv("buy_log.csv", dtype={"buy_uuid": str})
    except:
        buy_df = pd.DataFrame(columns=["market", "buy_uuid"])

    buy_df["uuid_str"] = buy_df["buy_uuid"].fillna("").astype(str).str.strip()

    # 시장별 buy_log uuid
    buy_log_map = {
        market: set(
            buy_df[
                (buy_df["market"] == market)
                & (buy_df["uuid_str"] != "")
            ]["uuid_str"].tolist()
        )
        for market in markets
    }

    # ======================================================
    # 3) sell_log.csv 로드
    # ======================================================
    try:
        sell_df = pd.read_csv("sell_log.csv", dtype={"sell_uuid": str})
    except:
        sell_df = pd.DataFrame(columns=["market", "sell_uuid"])

    sell_df["uuid_str"] = sell_df["sell_uuid"].fillna("").astype(str).str.strip()

    # 시장별 sell_log uuid
    sell_log_map = {
        market: set(
            sell_df[
                (sell_df["market"] == market)
                & (sell_df["uuid_str"] != "")
            ]["uuid_str"].tolist()
        )
        for market in markets
    }

    # ======================================================
    # 4) 시장별로 외부 주문 확인
    # ======================================================
    for market in markets:
        print(f"[cleanup] ▶ {market} 체크 중")

        tracked_buy = buy_log_map.get(market, set())
        tracked_sell = sell_log_map.get(market, set())

        # buy_log + sell_log → 추적 중인 전체 주문
        tracked_all = tracked_buy.union(tracked_sell)

        print(f"   - buy_log uuid: {tracked_buy}")
        print(f"   - sell_log uuid: {tracked_sell}")
        print(f"   - 추적 중인 전체 uuid: {tracked_all}")

        # 실제 전체 미체결 주문
        actual_open = set(get_all_open_buy_orders(market).keys())
        print(f"   - 실제 미체결 uuid: {actual_open}")

        # 추적하지 않은 외부 주문 = 취소 대상
        to_cancel = actual_open - tracked_all

        if not to_cancel:
            print(f"   ▶ {market} 외부 주문 없음")
            continue

        print(f"🛑 [cleanup] {market} 외부 미체결 주문 발견 → 취소: {to_cancel}")

        try:
            cancel_orders_by_uuids(list(to_cancel), market)
        except Exception as e:
            print(f"⚠ {market} 외부 주문 취소 실패: {e}")
