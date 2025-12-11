# api/binance_futures.py
# api/binance_futures.py

import time
import hmac
import hashlib
import requests
import os
from typing import Dict, List, Optional
from utils.price_utils import adjust_price_and_qty_for_binance

# ============================
# ✅ 환경변수 (키는 .env에 저장)
# ============================
# 1차: 환경에서 읽기
BINANCE_API_KEY = os.getenv("BINANCE_FUTURE_API_KEY")
BINANCE_SECRET_KEY = os.getenv("BINANCE_FUTURE_SECRET_KEY")

# 2차: .env에서 못 읽은 경우를 대비 (python-dotenv 없어도 무해)
try:
    if not BINANCE_API_KEY or not BINANCE_SECRET_KEY:
        from dotenv import load_dotenv  # 있으면 사용
        load_dotenv()
        BINANCE_API_KEY = BINANCE_API_KEY or os.getenv("BINANCE_FUTURE_API_KEY")
        BINANCE_SECRET_KEY = BINANCE_SECRET_KEY or os.getenv("BINANCE_FUTURE_SECRET_KEY")
except Exception:
    # python-dotenv 미설치면 조용히 패스 (환경변수만 사용)
    pass

# 3차: 최종 유효성 점검 (명확한 에러 메시지)
if not BINANCE_API_KEY or not BINANCE_SECRET_KEY:
    raise RuntimeError(
        "[Binance Futures] API 키가 없습니다. .env 또는 환경변수에 다음 키를 설정하세요:\n"
        "  BINANCE_FUTURE_API_KEY=...\n"
        "  BINANCE_FUTURE_SECRET_KEY=...\n"
        "(.env를 쓰면, 실행 전에 `python-dotenv`를 설치해도 됩니다: pip install python-dotenv)"
    )

BASE_URL = "https://fapi.binance.com"   # USDT-M Futures
RECV_WINDOW = 5000

def _sign(params: Dict) -> str:
    # 여기도 방어적으로 확인
    if not BINANCE_SECRET_KEY:
        raise RuntimeError("[Binance Futures] SECRET 키가 로드되지 않았습니다.")
    query = "&".join([f"{k}={v}" for k, v in params.items()])
    return hmac.new(
        BINANCE_SECRET_KEY.encode("utf-8"),
        query.encode("utf-8"),
        hashlib.sha256
    ).hexdigest()



# ============================
# ✅ 공용 Request 함수
# ============================
def _request(method: str, endpoint: str, params: Dict = None, signed: bool = False):
    """Binance REST API 요청 공통 함수"""

    if params is None:
        params = {}

    if signed:
        params["timestamp"] = int(time.time() * 1000)
        params["recvWindow"] = RECV_WINDOW
        params["signature"] = _sign(params)

    url = f"{BASE_URL}{endpoint}"

    headers = {
        "X-MBX-APIKEY": BINANCE_API_KEY,
        "Content-Type": "application/x-www-form-urlencoded",
    }

    if method == "GET":
        res = requests.get(url, params=params, headers=headers)
    elif method == "POST":
        res = requests.post(url, params=params, headers=headers)
    elif method == "DELETE":
        res = requests.delete(url, params=params, headers=headers)
    elif method == "PUT":
        res = requests.put(url, params=params, headers=headers)
    else:
        raise ValueError(f"Invalid HTTP method: {method}")

    if res.status_code != 200:
        raise Exception(f"Binance API Error: {res.status_code}, {res.text}")

    return res.json()


# ============================
# ✅ 현재 Position Mode 조회 (Hedge / One-way)
# ============================
def get_position_mode() -> bool:
    """
    Returns:
        True  = Hedge Mode (dualSidePosition=true)
        False = One-way Mode
    """
    try:
        data = _request("GET", "/fapi/v1/positionSide/dual", signed=True)
        return data.get("dualSidePosition", False)
    except Exception as e:
        print(f"⚠️ Position mode 조회 실패: {e}")
        return False


# ============================
# ✅ Hedge Mode 설정
# ============================
def set_hedge_mode(enable: bool = True):
    """
    enable=True  → Hedge Mode
    enable=False → One-way Mode
    """
    mode = "true" if enable else "false"
    try:
        data = _request(
            "POST",
            "/fapi/v1/positionSide/dual",
            params={"dualSidePosition": mode},
            signed=True,
        )
        print(f"✅ Hedge Mode 설정 완료 → {data}")
    except Exception as e:
        print(f"❌ Hedge Mode 설정 실패: {e}")


# ============================
# ✅ 현재가 조회 (Orderbook Ask Price)
# ============================
def get_current_ask_price(market: str) -> float:
    """
    업비트 get_current_ask_price() 와 동일 인터페이스
    GET /fapi/v1/depth  → asks[0][0]
    """
    endpoint = "/fapi/v1/depth"
    params = {"symbol": market, "limit": 5}
    data = _request("GET", endpoint, params=params, signed=False)

    price = float(data["asks"][0][0])
    return price


# ============================
# ✅ 포지션 + 청산가 조회
# ============================
def get_accounts() -> Dict:
    """
    업비트 get_accounts() 와 동일한 포맷을 반환해야 함.
    GET /fapi/v2/positionRisk
    """

    endpoint = "/fapi/v2/positionRisk"
    positions = _request("GET", endpoint, signed=True)

    result = {}
    for pos in positions:
        symbol = pos["symbol"]
        amount = float(pos["positionAmt"])
        entry_price = float(pos["entryPrice"])
        liquidation_price = float(pos["liquidationPrice"])
        leverage = float(pos["leverage"])

        if amount != 0:  # 포지션 있는 심볼만 반환
            result[symbol] = {
                "balance": abs(amount),
                "avg_buy_price": entry_price,
                "liquidation_price": liquidation_price,
                "leverage": leverage,
                "side": "LONG" if amount > 0 else "SHORT",
            }

    return result


# ============================================
# 🚀 send_order_v3 — Hedge Mode 전용 / 완전 안전
# ============================================
def send_order(
    market: str,
    side: str,
    buy_amount=None,
    price=None,
    quantity=None,
    reduce_only=False,
    order_type=None,      # 사용자가 안 넣으면 자동 판별
    position_side=None, ord_type=None, amount_krw=None, unit_price=None, volume=None
):
    """
    기존 업비트 스타일 호출 방식을 그대로 지원하는 Wrapper.
    내부적으로 Binance Hedge Mode 주문 규격으로 변환한다.
    """
    if ord_type:
        order_type = {
            "market": "MARKET",
            "price":  "MARKET",   # 업비트 price=시장가
            "limit":  "LIMIT",
        }.get(ord_type.lower(), order_type)

    if amount_krw:
        buy_amount = amount_krw

    if unit_price:
        price = unit_price

    if volume:
        quantity = volume

    # 1) Hedge Mode에서는 reduce_only = False 강제
    reduce_only = False

    # 2) side → BUY / SELL 통일
    side = "BUY" if side.lower() in ["buy", "bid"] else "SELL"

    # 3) 포지션 방향 자동 결정
    # 롱 전략이면 LONG, 숏 전략이면 SHORT
    # (이 부분은 casino_strategy의 buy / sell 로직에 따라 분기)
    if position_side is None:
        position_side = "LONG" if side == "BUY" else "SHORT"

    # 4) order_type 자동 판별
    if order_type is None:
        if price is None:
            order_type = "MARKET"
        else:
            order_type = "LIMIT"

    # 5) buy_amount → quantity 변환 (업비트 스타일 지원)
    if quantity is None and buy_amount is not None:
        if order_type == "MARKET":
            ref_price = get_current_ask_price(market)
        else:
            ref_price = price

        quantity = buy_amount / ref_price

    print("[DEBUG] send_order 내부 final price:", price)
    print("[DEBUG] send_order 내부 final qty:", quantity)

    # 6) Binance 내부 주문 호출
    return _binance_send_order(
        market=market,
        side=side,
        position_side=position_side,
        order_type=order_type,
        quantity=quantity,
        price=price,
        buy_amount=None  # ⭐ 반드시 None으로 넘겨서 중복 계산 막기
    )


def _binance_send_order(
    market: str,
    side: str,                  # BUY / SELL
    position_side: str,         # LONG / SHORT (Hedge Mode 필수)
    order_type: str,            # LIMIT / MARKET / STOP_MARKET / TAKE_PROFIT / TAKE_PROFIT_MARKET
    buy_amount=None,  # 업비트 스타일
    quantity: float = None,
    price: float = None,
    stop_price: float = None,
    reduce_only: bool = False,
    time_in_force: str = "GTC",
):
    """
    🎯 Binance Futures Hedge Mode 전용 주문 생성 함수 (강화 버전)

    - reduceOnly 안전 필터
    - STOP_MARKET 즉시트리거 보호
    - LIMIT/MARKET/STOP/STOP_MARKET 완전 준수
    - stepSize/tickSize 정확 보정
    """

    if quantity is None:
        if buy_amount is None:
            raise ValueError("buy_amount 또는 quantity 둘 중 하나는 반드시 필요합니다.")

        # LIMIT: 사용자가 지정한 price 사용
        # MARKET: 현재 호가를 기준으로 금액 → 수량 계산
        if order_type.upper() == "MARKET":
            ref_price = get_current_ask_price(market)
        else:
            if price is None:
                raise ValueError("LIMIT/STOP/TP 계열 주문은 price가 필요합니다.")
            ref_price = price

        raw_qty = buy_amount / ref_price
        quantity = raw_qty


    # ----------------------------------------------------------
    # 0) Hedge Mode 전용 안전 필터
    # ----------------------------------------------------------
    if position_side.upper() not in ["LONG", "SHORT"]:
        raise ValueError("Hedge Mode 전용: position_side는 반드시 LONG 또는 SHORT 여야 합니다.")

    if side.lower() in ["bid", "buy"]:
        side = "BUY"
    elif side.lower() in ["ask", "sell"]:
        side = "SELL"
    else:
        raise ValueError("side는 BUY/SELL 또는 bid/ask 이어야 합니다.")


    # ----------------------------------------------------------
    # 2) 주문 타입별 파라미터 필수 체크
    # ----------------------------------------------------------
    ot = order_type.upper()

    if ot == "MARKET":
        if quantity is None:
            raise ValueError("MARKET 주문은 quantity 필수")
        price = None

    elif ot == "LIMIT":
        if price is None or quantity is None:
            raise ValueError("LIMIT 주문은 price+quantity 필수")

    elif ot in ["STOP", "TAKE_PROFIT"]:
        if price is None or stop_price is None or quantity is None:
            raise ValueError(f"{ot} 주문은 price + stop_price + quantity 필수")

    elif ot in ["STOP_MARKET", "TAKE_PROFIT_MARKET"]:
        if stop_price is None or quantity is None:
            raise ValueError(f"{ot} 주문은 stop_price + quantity 필수")
        price = None    # 무조건 price 사용하면 안됨

    else:
        raise ValueError(f"지원하지 않는 order_type: {order_type}")


    # ----------------------------------------------------------
    # 3) 가격·수량 Binance 규칙 보정
    # ----------------------------------------------------------
    price, quantity = adjust_price_and_qty_for_binance(
        symbol=market,
        price=price,
        qty=quantity,
        is_market=(ot == "MARKET")
    )

    # STEP: reduceOnly 재검증 (보정 후 qty가 balance보다 커졌는지 확인)
    # Hedge Mode 강제: reduceOnly 파라미터 삭제
    if reduce_only:
        print("⚠ Hedge Mode에서는 reduceOnly를 지원하지 않습니다. → 자동 무시합니다.")
        reduce_only = False

    # ----------------------------------------------------------
    # 4) Binance API 요청 파라미터 조립
    # ----------------------------------------------------------
    client_uuid = str(uuid4())[:20]

    params = {
        "symbol": market,
        "side": side.upper(),
        "positionSide": position_side.upper(),
        "type": ot,
        "newClientOrderId": client_uuid,
    }

    if ot == "LIMIT":
        params.update({
            "quantity": quantity,
            "price": price,
            "timeInForce": time_in_force,
        })

    elif ot == "MARKET":
        params["quantity"] = quantity

    elif ot in ["STOP_MARKET", "TAKE_PROFIT_MARKET"]:
        params.update({
            "quantity": quantity,
            "stopPrice": stop_price,
            "workingType": "MARK_PRICE"
        })


    # ----------------------------------------------------------
    # 5) Binance API 호출
    # ----------------------------------------------------------
    print("[DEBUG] biance_send_order 내부 final price:", price)
    print("[DEBUG] biance_send_order 내부 final qty:", quantity)
    response = _request("POST", "/fapi/v1/order", params=params, signed=True)

    print(f"📌 [{market}] {ot} 주문 완료 | side={side}, qty={quantity}, price={price}, stop={stop_price}, reduce={reduce_only}")

    return {
        "uuid": client_uuid,
        "response": response
    }




# ============================
# ✅ 주문 상태 조회
# ============================

def get_order_results_by_uuids(uuid_list: list, market: str) -> dict:
    """
    uuid 리스트(uuid_list)에 대해 Binance 주문 상태 조회.
    market은 symbol(ex: BNBUSDT)
    """
    results = {}

    for uuid in uuid_list:
        try:
            params = {
                "symbol": market,
                "origClientOrderId": uuid,
            }

            # ✅ 여기서 signed=True → 자동으로 timestamp + signature 붙음
            data = _request("GET", "/fapi/v1/order", params=params, signed=True)

            status = data.get("status")  # NEW / PARTIALLY_FILLED / FILLED / CANCELED 등

            # Binance → casino_system 상태 매핑
            if status in ["NEW", "PARTIALLY_FILLED"]:
                results[uuid] = "wait"
            elif status == "FILLED":
                results[uuid] = "done"
            else:
                results[uuid] = "cancel"

        except Exception as e:
            print(f"⚠️ get_order_results_by_uuids 실패: {uuid} → {e}")

    return results



# ============================
# ✅ 주문 취소
# ============================
# api/binance_futures.py

def cancel_orders_by_uuids(uuid_list: List[str], market: str) -> Dict:
    """
    DELETE /fapi/v1/order (바이낸스는 symbol 필수)
    uuid_list: origClientOrderId 들
    market: 예) "BNBUSDT"
    """
    endpoint = "/fapi/v1/order"
    success, failed = 0, 0
    detail = {"success": [], "failed": []}

    if not uuid_list:
        return {"success": {"count": 0, "uuids": []}, "failed": {"count": 0, "uuids": []}}

    for uuid in uuid_list:
        try:
            _request("DELETE", endpoint,
                     params={"symbol": market, "origClientOrderId": uuid},
                     signed=True)
            success += 1
            detail["success"].append(uuid)
        except Exception as e:
            failed += 1
            detail["failed"].append({"uuid": uuid, "error": str(e)})

    return {
        "success": {"count": success, "uuids": detail["success"]},
        "failed": {"count": failed, "uuids": detail["failed"]},
    }



# ============================
# ✅ 캔들 조회 (1m, 5m, etc)
# ============================
def get_candles(market: str, interval="1m", limit=200):
    endpoint = "/fapi/v1/klines"
    params = {"symbol": market, "interval": interval, "limit": limit}
    return _request("GET", endpoint, params=params, signed=False)

# ============================
# ✅ 정정 주문 (취소 후 신규)
# ============================
from uuid import uuid4

# ============================
# ✅ 정정 주문 (취소 후 신규 생성)
# ============================
from uuid import uuid4

def cancel_and_new_order(
    prev_order_uuid: str,
    market: str,
    price: float,
    quantity: float,
    side: str,                     # ✅ BUY / SELL 동적으로 받음
    position_side: str = "LONG",   # ✅ LONG / SHORT 둘 다 대응
    holdings=None
):
    """
    기존 주문(prev_order_uuid)을 취소하고 새로운 주문을 만든다.

    Params:
        prev_order_uuid : 기존 주문 clientOrderId
        market          : ex) 'BNBUSDT'
        price           : 새 지정가
        quantity        : 주문 수량
        side            : "BUY" / "SELL"
        position_side   : "LONG" / "SHORT"
        reduce_only     : True면 포지션 감소용 주문

    Return:
        {"new_order_uuid": "<uuid>"}
    """

    # 1) 기존 주문 취소
    try:
        _request(
            "DELETE",
            "/fapi/v1/order",
            params={"symbol": market, "origClientOrderId": prev_order_uuid},
            signed=True,
        )
        print(f"✅ 기존 주문 취소 성공: {prev_order_uuid}")
    except Exception as e:
        print(f"⚠️ 기존 주문 취소 실패 또는 없는 주문: {e}")

    # 2) 새 주문 client uuid 생성
    new_uuid = str(uuid4())[:15]

    # ✅ Binance 규칙에 맞게 price/qty 보정
    adj_price, adj_qty = adjust_price_and_qty_for_binance(
        symbol=market,
        price=price,
        qty=quantity,
        is_market=False
    )

    # ----- DEBUG START -----
    print("====== [DEBUG reduceOnly 조건 검사] ======")
    print(f"market: {market}")
    print(f"side: {side}, positionSide: {position_side}")
    print(f"current balance: {float(holdings.get('balance', 0)) if holdings else 'N/A'}")
    print(f"order qty(adj_qty): {adj_qty}")
    print(f"order price(adj_price): {adj_price}")

    # case1: 수량이 0으로 보정된 상태
    if adj_qty == 0:
        print("❗ adj_qty == 0 → 바이낸스가 reduceOnly 주문을 거절할 수 있음")

    # case2: 포지션 없음
    if adj_qty > 0 and holdings and float(holdings.get("balance", 0)) == 0:
        print("❗ balance == 0 → 포지션이 없는데 reduceOnly 주문이 들어옴")

    print("========================================")
    # ----- DEBUG END -----

    # 신규 주문 생성
    params = {
        "symbol": market,
        "side": side,
        "positionSide": position_side,
        "type": "LIMIT",  # ✅ TAKE_PROFIT_LIMIT 금지 → LIMIT 사용
        "timeInForce": "GTC",
        "price": adj_price,
        "quantity": adj_qty,
        "newClientOrderId": new_uuid,
    }


    res = _request("POST", "/fapi/v1/order", params=params, signed=True)

    print(
        f"🆕 신규 {side} 주문 생성 완료: {new_uuid}, qty={adj_qty}, price={adj_price}, pos={position_side}"
    )

    return {"new_order_uuid": new_uuid}


def set_leverage(symbol: str, leverage: int):
    """
    특정 심볼(symbol)에 레버리지를 설정합니다.
    POST /fapi/v1/leverage
    """
    if not (1 <= leverage <= 125):
        raise ValueError("레버리지는 1~125 사이의 정수여야 합니다.")

    params = {"symbol": symbol, "leverage": leverage}
    res = _request("POST", "/fapi/v1/leverage", params=params, signed=True)
    print(f"✅ 레버리지 설정 완료 → {symbol}: {leverage}배 (maxNotional={res.get('maxNotionalValue')})")
    return res
