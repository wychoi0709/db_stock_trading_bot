import pandas as pd
from api import get_accounts, get_current_ask_price

def get_coin_units(buy_log_df, market):
    """ 특정 코인(market)의 filled == done 인 buy_units 합계를 반환 """
    if buy_log_df is None or buy_log_df.empty:
        return 0

    logs = buy_log_df[
        (buy_log_df["market"] == market) &
        (buy_log_df["filled"] == "done")
    ]

    return logs["buy_units"].astype(float).sum()


def generate_buy_orders(setting_df: pd.DataFrame, buy_log_df: pd.DataFrame, current_prices: dict, mode="normal") -> pd.DataFrame:
    """
    카지노 매매 전략에 따라 상황을 판단하고,
    각 상황에 따른 매수 주문 내역을 buy_log 형태로 생성/수정하여 리턴한다.
    """
    print("[casino_strategy.py] generate_buy_orders() 호출됨")

    new_logs = []

    for _, setting in setting_df.iterrows():
        market = setting["market"]
        unit_size = setting["unit_size"]
        small_pct = setting["small_flow_pct"]
        small_units = setting["small_flow_units"]
        large_pct = setting["large_flow_pct"]
        large_units = setting["large_flow_units"]

        coin_logs = buy_log_df[buy_log_df["market"] == market]
        initial_logs = coin_logs[coin_logs["buy_type"] == "initial"]
        flow_logs = coin_logs[coin_logs["buy_type"].isin(["small_flow", "large_flow"])]

        # -----------------------------
        # 전량 매도시 즉시 매수
        # -----------------------------
        if mode == "initial_only":
            print(f"🎯 {market} → 전량 매도 후 initial 매수만 생성")

            # ⚠️ 이미 initial 주문이 존재하면 신규 생성 금지
            if not coin_logs.empty:
                if any(coin_logs["buy_type"] == "initial"):
                    print(f"⏸ {market} 이미 initial 주문 존재 → 신규 생성 안함")
                    continue

            current_price = current_prices.get(market)
            if current_price is None:
                print(f"❌ {market} 현재가 없음 → 건너뜀")
                continue

            new_logs.append({
                "time": pd.Timestamp.now(),
                "market": market,
                "target_price": current_price,
                "buy_amount": unit_size,
                "buy_units": 1,
                "buy_type": "initial",
                "buy_uuid": None,
                "filled": "update"
            })
            continue

        current_price = current_prices.get(market)
        if current_price is None:
            print(f"❌ 현재 가격 없음 → {market}")
            continue


        # 수정된 부분 (generate_buy_orders 내부)

        # ✅ [상황1] 최초 주문 없음
        if flow_logs.empty:

            # 데이터 2 - small_flow
            small_price = round(current_price * (1 - small_pct), 2)
            new_logs.append({
                "time": pd.Timestamp.now(),
                "market": market,
                "target_price": small_price,
                "buy_amount": unit_size * small_units,
                "buy_units": small_units,
                "buy_type": "small_flow",
                "buy_uuid": None,
                "filled": "update"  # 수정됨
            })

            # 데이터 3 - large_flow
            large_price = round(current_price * (1 - large_pct), 2)
            new_logs.append({
                "time": pd.Timestamp.now(),
                "market": market,
                "target_price": large_price,
                "buy_amount": unit_size * large_units,
                "buy_units": large_units,
                "buy_type": "large_flow",
                "buy_uuid": None,
                "filled": "update"  # 수정됨
            })

        # ✅ 수정된 상황2: initial filled == done인 코인
        elif not initial_logs.empty:
            print(f"📌 {market} → 수정된 상황2: flow 주문 개별 처리 시작")

            for _, row in flow_logs.iterrows():
                buy_type = row["buy_type"]
                target_price = row["target_price"]
                raw_filled = row["filled"]
                filled = "" if pd.isna(raw_filled) else str(raw_filled).strip()
                row_index = row.name

                if pd.isna(target_price) or pd.isna(row["buy_amount"]) or pd.isna(row["buy_units"]):
                    raise ValueError(f"[❌ 에러] {market} - {buy_type} 주문에 누락된 값이 있습니다. 행: {row.to_dict()}")

                target_price = float(target_price)
                unit_pct = small_pct if buy_type == "small_flow" else large_pct


                # ============================================================
                # ⭐ PATCH 2 — 폐장 후 개장 시 uuid/reset 상태 처리
                # 조건: uuid=None & filled=""
                # 로직: 기존 target_price와 현재가격 비교
                # ============================================================
                if pd.isna(row["buy_uuid"]) and filled == "":
                    original = float(target_price)

                    # 1) 현재가격이 기존 target_price보다 낮으면 → 재설정
                    if current_price < original:
                        new_target = round(current_price * (1 - unit_pct), 2)
                        print(f"🌅 {market} {buy_type} → 개장 후 가격 재산출: 기존={original}, 새={new_target}")
                        buy_log_df.loc[row_index, "target_price"] = new_target
                        buy_log_df.loc[row_index, "filled"] = "update"

                    # 2) 현재가격이 기존 target_price보다 높으면 → 기존 유지
                    else:
                        print(f"🌅 {market} {buy_type} → 기존 가격 유지: 기존={original}, 현재가={current_price}")
                        # 그래도 filled는 update로 바꿔줘야 매수 주문 들어감
                        buy_log_df.loc[row_index, "filled"] = "update"
                    continue

                # case1: wait 상태 → 가격 상향 후 재조정
                if filled == "wait":
                    # 가격이 기준 이상으로 상승한 경우 → 매수 기준 재조정
                    base = target_price / (1 - unit_pct)
                    rise_trigger = base * (1 + unit_pct / 2)

                    if current_price > rise_trigger:
                        new_price = round(rise_trigger * (1 - unit_pct), 2)
                        print(f"↗ {market} {buy_type} 가격 재조정: {target_price} → {new_price}")
                        buy_log_df.loc[row_index, "target_price"] = new_price
                        buy_log_df.loc[row_index, "filled"] = "update"


                # case2: done 상태 → 동일 비율로 다시 내려서 주문 재생성
                elif filled == "done":
                    # 이 칸은 새 주문으로 취급하므로 uuid 초기화
                    buy_log_df.at[row_index, "buy_uuid"] = None

                    # 1) 기존 로직 기준으로 "다음 한 칸" 가격 N 계산
                    default_next_price = round(target_price * (1 - unit_pct), 2)

                    # 2) 현재가 P
                    P = current_price
                    N = default_next_price

                    # 2-1) 급락이 아니라면 → 기존처럼 이 행만 한 칸 내리기
                    if P >= N:
                        new_price = N
                        print(
                            f"🔁 {market} {buy_type} 연속 주문(기존 로직): "
                            f"{target_price} → {new_price}"
                        )
                        buy_log_df.loc[row_index, "target_price"] = new_price
                        buy_log_df.loc[row_index, "filled"] = "update"

                    # 2-2) 급락(P < N) 이라면 → P 를 기준으로 small/large 둘 다 재설계
                    else:
                        print(
                            f"📉 {market} {buy_type} 체결 후 급락 감지 "
                            f"(N={N}, P={P}) → 현재가 기준으로 small/large 재설정"
                        )

                        base_price = P

                        # 이 코인에 대한 small / large 행의 인덱스 찾기
                        small_idx = flow_logs[flow_logs["buy_type"] == "small_flow"].index
                        large_idx = flow_logs[flow_logs["buy_type"] == "large_flow"].index

                        # P 기준으로 새 small / large 가격 계산
                        new_small_price = round(base_price * (1 - small_pct), 2)
                        new_large_price = round(base_price * (1 - large_pct), 2)

                        # small_flow 갱신
                        if not small_idx.empty:
                            old_small = buy_log_df.loc[small_idx[0], "target_price"]
                            print(
                                f"   ↪ small_flow: {old_small} → {new_small_price}"
                            )
                            buy_log_df.loc[small_idx, "target_price"] = new_small_price
                            buy_log_df.loc[small_idx, "filled"] = "update"
                            # small 이 방금 체결된 칸일 수도 있으니 uuid 초기화
                            buy_log_df.loc[small_idx, "buy_uuid"] = None

                        # large_flow 갱신
                        if not large_idx.empty:
                            old_large = buy_log_df.loc[large_idx[0], "target_price"]
                            print(
                                f"   ↪ large_flow: {old_large} → {new_large_price}"
                            )
                            buy_log_df.loc[large_idx, "target_price"] = new_large_price
                            buy_log_df.loc[large_idx, "filled"] = "update"
                            # large 가 방금 체결된 칸일 수도 있으니 uuid 초기화
                            buy_log_df.loc[large_idx, "buy_uuid"] = None



                elif pd.isna(filled) or filled == "":
                    print(f"📝 {market} {buy_type} 수동 주문 → 필드 유효성 검사")

                    # 필수 항목 확인: market, target_price, buy_amount, buy_units, buy_type
                    required_columns = ["market", "target_price", "buy_amount", "buy_units", "buy_type"]
                    missing_columns = [col for col in required_columns if pd.isna(row[col]) or row[col] == ""]

                    if missing_columns:
                        raise ValueError(f"[❌ 에러] {market} - {buy_type} 수동 주문에 누락된 필드가 있습니다: {missing_columns}")

                    # 이상 없으면 update 처리
                    # buy_log_df.loc[row_index, "filled"] = "update"


                # case4: cancel 등 기타 상태 → 예외 처리
                else:
                    raise ValueError(f"[❌ 에러] {market} - {buy_type} 주문의 filled 상태가 예외적입니다: '{filled}'")

    # 새로운 주문이 있다면 기존 로그와 결합
    if new_logs:
        new_df = pd.DataFrame(new_logs, dtype=object)
        buy_log_df = pd.concat([buy_log_df, new_df], ignore_index=True)

    return buy_log_df


def generate_sell_orders(setting_df: pd.DataFrame, holdings: dict, sell_log_df: pd.DataFrame) -> pd.DataFrame:
    print("[casino_strategy.py] generate_sell_orders() 호출됨")

    # 기존 sell_log_df를 복사해서 시작
    updated_df = sell_log_df.copy()

    for _, row in setting_df.iterrows():
        market = row["market"]

        # 보유 중인 코인만 대상
        if market not in holdings:
            continue

        h = holdings[market]

        avg_buy_price = round(h["avg_price"], 8)
        quantity = round(h["balance"] + h["locked"], 8)

        if quantity <= 0:
            continue  # 보유 수량이 없으면 매도할 이유 없음

        take_profit_pct = row["take_profit_pct"]
        target_price = round(avg_buy_price * (1 + take_profit_pct), 2)

        # ⭐ 현재가격 조회
        market_code = row["market_code"]
        current_price = get_current_ask_price(market=market, market_code=market_code)

        # ⭐ 갭 상승 체크 로직
        if current_price is not None and current_price > target_price:
            print(f"🚀 {market} 갭 상승 감지! 목표가 {target_price} → 현재가 {current_price} 로 매도가 변경")
            target_price = round(current_price, 2)

        # 기존 sell_log에서 해당 market 데이터 있는지 확인
        existing_idx = updated_df[updated_df["market"] == market].index

        if not existing_idx.empty:
            idx = existing_idx[0]
            existing = updated_df.loc[idx]

            is_same = (
                round(existing["avg_buy_price"], 8) == avg_buy_price and
                round(existing["quantity"], 8) == quantity and
                round(existing["target_sell_price"], 2) == target_price
            )

            if is_same:
                print(f"✅ {market} → 보유 정보와 동일 → 유지")
                continue

            print(f"✏️ {market} → 기존과 차이 있음 → 수정")
            updated_df.loc[idx, "avg_buy_price"] = avg_buy_price
            updated_df.loc[idx, "quantity"] = quantity
            updated_df.loc[idx, "target_sell_price"] = target_price
            updated_df.loc[idx, "filled"] = "update"

        else:
            print(f"🆕 {market} → 새로운 sell_log 생성")
            new_row = {
                "market": market,
                "avg_buy_price": avg_buy_price,
                "quantity": quantity,
                "target_sell_price": target_price,
                "sell_uuid": None,
                "filled": "update"
            }
            updated_df = pd.concat([updated_df, pd.DataFrame([new_row])], ignore_index=True)

    return updated_df


