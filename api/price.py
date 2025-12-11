# api/price.py

import requests
from typing import List, Dict, Optional


def get_second_candles(market: str, to: Optional[str] = None, count: int = 1) -> List[Dict]:
    print(f"[price.py] get_second_candles() 실행됨 - market={market}, count={count}")

    url = "https://api.upbit.com/v1/candles/seconds"
    headers = {"accept": "application/json"}
    params = {
        "market": market,
        "count": count
    }

    if to:
        params["to"] = to

    response = requests.get(url, params=params, headers=headers)

    if response.status_code != 200:
        print("[price.py] 초봉 조회 실패")
        raise Exception(f"초봉 조회 실패: {response.status_code}, {response.text}")

    print(f"[price.py] 초봉 조회 성공 - {len(response.json())}개 데이터")
    return response.json()


def get_current_ask_price(market: str) -> float:
    """
    업비트 호가 정보 중 최우선 매도호가(ask_price)를 반환
    """
    print(f"[price.py] get_current_ask_price() 실행됨 - market={market}")

    url = "https://api.upbit.com/v1/orderbook"
    headers = {"accept": "application/json"}
    params = {"markets": market}

    response = requests.get(url, headers=headers, params=params)

    if response.status_code != 200:
        raise Exception(f"[호가 조회 실패] {response.status_code} - {response.text}")

    data = response.json()
    orderbook_units = data[0].get("orderbook_units", [])

    if not orderbook_units:
        raise Exception("[호가 데이터 없음]")

    ask_price = orderbook_units[0]["ask_price"]  # 최우선 매도 호가
    print(f"[price.py] 매도 호가: {ask_price}")
    return ask_price


def get_minute_candles(market: str, unit: int = 1, to: Optional[str] = None, count: int = 1) -> List[Dict]:
    """
    업비트에서 분(Minute) 단위 캔들 데이터를 가져옵니다.

    :param market: 마켓 코드 (예: KRW-BTC)
    :param unit: 분 단위 (1, 3, 5, 10, 15, 30, 60, 240)
    :param to: 마지막 캔들 시각 (exclusive) - ISO8601 포맷 문자열
    :param count: 요청할 캔들 개수 (최대 200)
    :return: 캔들 리스트 (dict)
    """
    print(f"[price.py] get_minute_candles() 실행됨 - market={market}, unit={unit}, count={count}, to={to}")

    url = f"https://api.upbit.com/v1/candles/minutes/{unit}"
    headers = {"accept": "application/json"}
    params = {
        "market": market,
        "count": count
    }

    if to:
        params["to"] = to

    response = requests.get(url, headers=headers, params=params)

    if response.status_code != 200:
        raise Exception(f"[분봉 조회 실패] {response.status_code} - {response.text}")

    print(f"[price.py] 분봉 캔들 조회 성공 - {len(response.json())}개 데이터")
    return response.json()