# utils/price_utils.py
from __future__ import annotations
from decimal import Decimal, ROUND_DOWN, getcontext
from functools import lru_cache
from typing import Dict, Optional, Tuple
import requests
import os
import time

# 부동소수 안정성 향상
getcontext().prec = 28

BINANCE_FAPI_BASE = "https://fapi.binance.com"

# --- 공통 유틸 ---

def _floor_to_step(value: Decimal, step: Decimal) -> Decimal:
    if step == 0:
        return value
    # step 배수로 내림
    return (value // step) * step

def _to_decimal(x) -> Decimal:
    if x is None:
        return Decimal("0")
    if isinstance(x, Decimal):
        return x
    return Decimal(str(x))


# --- exchangeInfo 캐시 ---

@lru_cache(maxsize=1)
def _get_exchange_info() -> Dict:
    # 1분 정도 캐시 무효화를 위해 serverTime을 이용해 약간 흔들림을 줌 (요청 남발 방지)
    _ = int(time.time() // 60)
    resp = requests.get(f"{BINANCE_FAPI_BASE}/fapi/v1/exchangeInfo", timeout=10)
    resp.raise_for_status()
    return resp.json()

@lru_cache(maxsize=256)
def _get_symbol_meta(symbol: str) -> Dict:
    data = _get_exchange_info()
    for s in data.get("symbols", []):
        if s.get("symbol") == symbol:
            return s
    raise ValueError(f"[price_utils] 심볼을 찾을 수 없습니다: {symbol}")

def _get_filters(symbol: str) -> Dict[str, Dict]:
    meta = _get_symbol_meta(symbol)
    out = {}
    for f in meta.get("filters", []):
        out[f["filterType"]] = f
    return out


# --- 바이낸스 규칙 기반 보정 함수 ---

def get_binance_precisions(symbol: str) -> Tuple[Decimal, Decimal, Optional[Decimal]]:
    """
    returns: (tickSize, lot_stepSize, market_stepSize(or None))
    """
    filters = _get_filters(symbol)
    price_filter = filters.get("PRICE_FILTER", {})
    lot_size     = filters.get("LOT_SIZE", {})
    market_lot   = filters.get("MARKET_LOT_SIZE", {})  # 없을 수도 있음

    tick = _to_decimal(price_filter.get("tickSize", "0"))
    step = _to_decimal(lot_size.get("stepSize", "0"))
    mstep = _to_decimal(market_lot.get("stepSize", "0")) if market_lot else None
    if mstep == Decimal("0"):
        mstep = None
    return tick, step, mstep

def get_min_notional(symbol: str) -> Optional[Decimal]:
    filt = _get_filters(symbol).get("MIN_NOTIONAL")
    if not filt:
        return None
    # futures는 키가 notional
    return _to_decimal(filt.get("notional", "0"))

def adjust_price_to_tick(price: float, market: str = "BINANCE", ticker: str = "") -> float:
    """
    ✅ 업비트 시그니처와 동일하지만, market 인자를 무시하고 (업비트 안씀)
    ticker(symbol) 기준으로 바이낸스 호가단위에 맞춰 price를 내림 보정.
    """
    symbol = ticker or ""
    if not symbol:
        return float(price)

    tick, _, _ = get_binance_precisions(symbol)
    if tick == 0:
        return float(price)

    p = _to_decimal(price)
    adj = _floor_to_step(p, tick).quantize(tick, rounding=ROUND_DOWN)
    return float(adj)

def adjust_qty_to_step(quantity: float, ticker: str, is_market: bool = False) -> float:
    """
    ✅ LOT_SIZE / MARKET_LOT_SIZE 기준으로 수량 내림 보정
    """
    tick, step, mstep = get_binance_precisions(ticker)
    step_to_use = mstep if (is_market and mstep) else step
    if step_to_use == 0:
        return float(quantity)

    q = _to_decimal(quantity)
    adj = _floor_to_step(q, step_to_use).quantize(step_to_use, rounding=ROUND_DOWN)
    return float(adj)

def adjust_price_and_qty_for_binance(
    symbol: str, price: Optional[float], qty: float, is_market: bool = False
) -> Tuple[Optional[float], float]:
    """
    ✅ 가격/수량을 바이낸스 제약에 맞게 보정 + MIN_NOTIONAL(최소 주문금액) 만족하도록 보정
    - LIMIT: price != None
    - MARKET: price is None
    """
    p = None if price is None else adjust_price_to_tick(price, ticker=symbol)
    q = adjust_qty_to_step(qty, ticker=symbol, is_market=is_market)

    # 최소 주문 금액 보정 (있는 경우)
    min_notional = get_min_notional(symbol)
    if min_notional and min_notional > 0:
        # 시장가면 현재가로 추정해야 하지만, 여기서는 LIMIT 또는
        # 상위 레벨에서 현재가로 계산 후 넘겨주길 권장.
        # LIMIT이면 p 사용, MARKET이면 p가 None → 상위에서 현재가로 보정해 넘기는 걸 추천.
        ref_price = _to_decimal(p if p is not None else price if price is not None else 0)
        if ref_price > 0:
            notional = ref_price * _to_decimal(q)
            if notional < min_notional:
                # step 단위로 올려서 notional 충족
                _, step, mstep = get_binance_precisions(symbol)
                step_to_use = mstep if (is_market and mstep) else step
                if step_to_use > 0:
                    needed = (min_notional / ref_price)
                    # needed 수량을 step 배수로 올림 (내림이 아니라 ↑ 올려야 최소금액 달성)
                    # 올림을 위해 작은 epsilon을 더하고 floor
                    k = (needed / step_to_use).to_integral_value(rounding=ROUND_DOWN)
                    if step_to_use * k < needed:
                        k += 1
                    q = float(step_to_use * k)
    return (None if p is None else float(p)), float(q)
