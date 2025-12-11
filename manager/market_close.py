# manager/market_close.py

import pandas as pd
import os
from api import get_accounts

def close_market_cleanup():
    """
    폐장 시 buy_log.csv 정리
    - 보유 중인 종목:
        → initial: 유지
        → small/large: uuid=None, filled=""
    - 보유하지 않은 종목:
        → 해당 market 모든 로그 삭제
    """
    print("🕛 [폐장 처리 시작] buy_log 정리 중...")

    if not os.path.exists("buy_log.csv"):
        print("❌ buy_log.csv 없음 → 종료")
        return

    try:
        buy_log_df = pd.read_csv("buy_log.csv", dtype={"buy_uuid": str})
    except Exception as e:
        print(f"❌ buy_log.csv 읽기 실패: {e}")
        return

    accounts = get_accounts()
    holdings = set(accounts.keys())

    cleaned_rows = []

    for market in buy_log_df["market"].unique():

        market_logs = buy_log_df[buy_log_df["market"] == market].copy()

        # 보유하지 않은 종목
        if market not in holdings:
            print(f"🗑️ [{market}] 보유하지 않음 → 모든 로그 삭제")
            continue

        # 보유 중인 종목 → initial 유지, flow reset
        for idx, row in market_logs.iterrows():
            buy_type = row["buy_type"]

            if buy_type == "initial":
                cleaned_rows.append(row)
            else:
                row["buy_uuid"] = ""
                row["filled"] = ""
                cleaned_rows.append(row)

    # 재구성 & 저장
    new_df = pd.DataFrame(cleaned_rows) if cleaned_rows else pd.DataFrame(columns=buy_log_df.columns)
    tmp = "buy_log.csv.tmp"
    new_df.to_csv(tmp, index=False)
    os.replace(tmp, "buy_log.csv")

    print("🎉 폐장 처리 완료 → buy_log.csv 업데이트 완료")
