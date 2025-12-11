# main.py

import os
import sys
import pandas as pd
from strategy.entry import run_casino_entry
from api import _get_token


# 필요 열 정의
REQUIRED_COLUMNS = {
    "setting.csv": [
        "market", "unit_size", "small_flow_pct", "small_flow_units",
        "large_flow_pct", "large_flow_units", "take_profit_pct", "leverage","market_code"
    ],
    "buy_log.csv": [
        "time", "market", "target_price", "buy_amount",
        "buy_units", "buy_type", "buy_uuid", "filled"
    ],
    "sell_log.csv": [
        "market", "avg_buy_price", "quantity", "target_sell_price", "sell_uuid", "filled"
    ],
}


def ensure_csv_files():
    print("[main.py] CSV 파일 검사 시작")

    for filename, expected_columns in REQUIRED_COLUMNS.items():
        if not os.path.exists(filename):
            print(f"📄 '{filename}' 파일이 없어 새로 생성합니다.")
            df = pd.DataFrame(columns=expected_columns)
            df.to_csv(filename, index=False)
        else:
            df = pd.read_csv(filename)
            existing_columns = df.columns.tolist()
            if existing_columns != expected_columns:
                print(f"❌ '{filename}' 파일의 열이 예상과 다릅니다.")
                print(f"    ▶ 예상: {expected_columns}")
                print(f"    ▶ 실제: {existing_columns}")
                print("🚫 프로그램을 종료합니다.")
                sys.exit(1)
            else:
                print(f"✅ '{filename}' 파일이 정상입니다.")

def main():
    # main.py
    print("[main.py] 프로그램 시작")

    ensure_csv_files()

    # ✅ setting.csv 불러오기
    setting_df = pd.read_csv("setting.csv")
    # ✅ 토큰 1회 갱신
    try:
        _get_token()
        print("🔑 [KIS] 토큰 발급 완료")
    except Exception as e:
        print(f"❌ [KIS] 토큰 발급 실패: {e}")
        sys.exit(1)

    run_casino_entry()
    print("[main.py] 프로그램 종료")


if __name__ == "__main__":
    main()


#
# from manager.simulator import simulate_with_strategy
#
# simulate_with_strategy(
#     market="KRW-DOGE",
#     start="2021-05-08 09:00",
#     end="2021-06-25 12:00",
#     unit=1,
#     unit_size=5000,
#     small_flow_pct=0.04,
#     small_flow_units=2,
#     large_flow_pct=0.13,
#     large_flow_units=14,
#     take_profit_pct=0.00575
# )