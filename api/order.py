# api/order.py

import requests
from api.auth import generate_jwt_token
from urllib.parse import urlencode
import config


def send_order(market: str, side: str, ord_type: str,
               amount_krw: float = None, unit_price: float = None,
               volume: float = None, time_in_force: str = None) -> dict:
    print(f"[order.py] send_order() 호출됨 - market={market}, side={side}, ord_type={ord_type}")

    params = {
        "market": market,
        "side": side,
        "ord_type": ord_type,
    }

    if ord_type == "price" and amount_krw is not None:
        params["price"] = str(amount_krw)

    if ord_type in {"limit", "best"} and unit_price is not None:
        params["price"] = str(unit_price)

    if ord_type in {"limit", "market", "best"} and side == "ask" and volume is not None:
        params["volume"] = str(volume)

    if ord_type in {"limit", "best"} and side == "bid" and volume is not None:
        params["volume"] = str(volume)

    if time_in_force:
        params["time_in_force"] = time_in_force

    headers = {
        "Authorization": generate_jwt_token(params),
        "Content-Type": "application/json"
    }

    response = requests.post(f"{config.SERVER_URL}/v1/orders", json=params, headers=headers)

    if response.status_code == 201:
        print("[order.py] 주문 성공")
        return response.json()
    else:
        print("[order.py] 주문 실패")
        raise Exception(f"[주문 실패] {response.status_code} - {response.text}")



def get_order_results_by_uuids(uuids: list[str]) -> dict:
    """
    여러 uuid에 대해 주문 상태를 한 번에 조회
    반환: {uuid: 상태} 딕셔너리
    """
    print(f"[order.py] get_order_results_by_uuids 호출됨 (개수: {len(uuids)})")

    params = {'uuids[]': uuids}
    headers = {
        "Authorization": generate_jwt_token(params),
        "Content-Type": "application/json"
    }

    response = requests.get(
        f"{config.SERVER_URL}/v1/orders/uuids",
        headers=headers,
        params=params
    )

    if response.status_code == 200:
        results = response.json()
        return {item['uuid']: item.get('state', 'unknown') for item in results}
    else:
        raise Exception(f"[주문 리스트 조회 실패] {response.status_code} - {response.text}")


def cancel_and_new_order(prev_order_uuid: str, market: str, price: float, amount: float) -> dict:
    print(f"[order.py] cancel_and_new_order 호출됨 - market={market}, prev_uuid={prev_order_uuid}")

    headers = {
        "Authorization": generate_jwt_token({
            "prev_order_uuid": prev_order_uuid,
            "new_ord_type": "limit",
            "new_price": str(price),
            "new_volume": str(amount),
        }),
        "Content-Type": "application/json"
    }

    payload = {
        "prev_order_uuid": prev_order_uuid,
        "new_ord_type": "limit",
        "new_price": str(price),
        "new_volume": str(amount),
        # "new_identifier": 생략됨
    }

    response = requests.post(f"{config.SERVER_URL}/v1/orders/cancel_and_new", json=payload, headers=headers)

    if response.status_code == 200:
        return response.json()
    else:
        raise Exception(f"[정정 주문 실패] {response.status_code} - {response.text}")





def cancel_orders_by_uuids(uuids: list[str]) -> dict:
    print(f"[order.py] cancel_orders_by_uuids() 호출됨 - 총 {len(uuids)}개")
    if not uuids:
        return {}

    params = {'uuids[]': uuids}
    headers = {
        "Authorization": generate_jwt_token(params),
        "accept": "application/json"
    }

    response = requests.delete(
        url=f"{config.SERVER_URL}/v1/orders/uuids",
        params=params,
        headers=headers
    )

    if response.status_code == 200:
        return response.json()
    else:
        raise Exception(f"[일괄 주문 취소 실패] {response.status_code} - {response.text}")
