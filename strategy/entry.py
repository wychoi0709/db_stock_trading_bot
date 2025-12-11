# strategy/entry.py

import time
from datetime import datetime

from api import is_us_market_open
from strategy.buy_entry import (
    run_buy_generate_flow,
    detect_filled_buy_orders,
    load_setting_data,
    process_sold_out_markets_for_initial,
)
from strategy.sell_entry import (
    immediate_sell_for_filled_buys,
    periodic_sell_status_check,   # 👉 추가
)
from manager.market_close import close_market_cleanup   # ⭐ 추가
from manager.order_cleanup import cleanup_untracked_buy_orders

# ⭐ 한국투자증권 해외주식 '장마감/시간외' 오류 패턴
MARKET_CLOSED_KEYWORDS = [
    "거래시간이 초과되었습니다",
    "거래가능 시간이 아닙니다",
    "장마감",
    "주문이 불가",
    "해당 시간에는",
    "허용되지 않습니다"
]

def run_casino_entry():
    print("[entry.py] ▶ 카지노 매매 시스템 시작")

    open_now = True
    last_minute_exec = time.time()
    market_closed_cleanup_done = False   # ⭐ 추가

    # 최초 setting 로드 (필요하다면 1분마다 갱신해도 됨)
    setting_df = load_setting_data()

    print("[entry.py] ▶ 초기화 완료. 메인 루프 진입")
    print(f"[entry.py] ▶ 초기 open_now={open_now}")

    while True:
        loop_start = time.time()
        now_str = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        print(f"\n[entry.py][LOOP] ===== 루프 시작: {now_str} =====")
        print(f"[entry.py][LOOP] open_now={open_now}, last_minute_exec={last_minute_exec}, loop_start={loop_start}")

        # =====================================================
        # ① 장이 열린 상태
        # =====================================================
        if open_now:
            # 만약 전에 폐장_cleanup이 실행된 상태라면 → 초기화
            if market_closed_cleanup_done:
                print("🔄 개장 감지 → 폐장 cleanup flag 초기화")
                market_closed_cleanup_done = False


            try:
                # (1) 전량 매도 후 initial 재진입(초단위)
                process_sold_out_markets_for_initial(setting_df)

                # (2) 1분 단위 매수 생성 (small/large 포함)
                elapsed = loop_start - last_minute_exec
                print(f"[entry.py][LOOP][OPEN] 1분 경과 체크: elapsed={elapsed:.2f}")

                if elapsed >= 60:
                    print("\n==============================================")
                    print(f"[entry.py][1-MIN] 1분 경과 → run_buy_generate_flow() 실행 at {now_str}")
                    print("==============================================")
                    run_buy_generate_flow()
                    last_minute_exec = loop_start

                # (3) 초단위 매수 체결 감지 → 즉시 매도
                filled_events = detect_filled_buy_orders()
                if filled_events:
                    immediate_sell_for_filled_buys(setting_df, filled_events)

                periodic_sell_status_check()

                # (4) 1초 대기
                time.sleep(1)

                try:
                    cleanup_untracked_buy_orders()
                except Exception as e:
                    print(f"[cleanup][ERROR] 외부 주문 정리 실패: {e}")

            except Exception as e:
                print(f"[entry.py][OPEN][EXCEPTION] 예외 발생: {e}")

                if "MARKET_CLOSED" in str(e):
                    print(f"⏸️ [entry.py][OPEN] 폐장 감지 → open_now=False 전환 ({e})")
                    open_now = False

                    # ⭐ 여기서 폐장 cleanup 실행 (단 1회)
                    if not market_closed_cleanup_done:
                        close_market_cleanup()
                        market_closed_cleanup_done = True

                else:
                    print(f"[entry.py][OPEN] ⚠ 일반 예외 → 1초 대기 후 재실행")
                    time.sleep(1)

        # =====================================================
        # ② 장이 닫힌 상태(open_now = False) → 개장 여부 체크
        # =====================================================
        else:
            print("[entry.py][LOOP][CLOSED] 장 닫힘 상태. 개장 여부 체크.")
            try:
                print("[entry.py][CLOSED] is_us_market_open() 호출")
                if is_us_market_open(market="GGLL"):
                    print("✅ [entry.py][CLOSED] 미국장 개장 감지 → open_now=True 전환")
                    open_now = True
                    # 개장 직후 다시 setting 갱신
                    setting_df = load_setting_data()
                    last_minute_exec = time.time()
                    continue
                else:
                    print("[entry.py][CLOSED] 아직 미개장 → 60초 대기")
                    time.sleep(60)

            except Exception as e:
                print(f"[entry.py][CLOSED][EXCEPTION] 개장 여부 확인 실패: {e}")
                print("[entry.py][CLOSED] 60초 대기 후 재시도")
                time.sleep(60)
