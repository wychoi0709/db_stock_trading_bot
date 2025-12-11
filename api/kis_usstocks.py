"""
한국투자증권(KIS) – 미국주식 어댑터 (최종 실행 버전)
TR-ID, 엔드포인트를 실제 문서 기준으로 반영.
Binance API 대체 호환 버전으로 프로젝트 내 시그니처 동일.
"""

import os
import time
import math
import json
import requests
from typing import Dict, List, Optional
from datetime import datetime, timedelta
import pytz
from dotenv import load_dotenv

from utils.kis_utils import normalize_uuid

load_dotenv()  # ✅ .env 파일 자동 로드


# 파일 상단 어딘가에 추가
class MarketClosedError(Exception):
    """미국장 폐장/휴장/비개장 시간 등 거래 불가 상태"""
    pass

class TokenExpiredError(Exception):
    """KIS 액세스 토큰 만료/권한 오류"""
    pass

# ---------------------------------------------------------
# 환경 변수 / 기본 설정
# ---------------------------------------------------------
IS_DEMO   = os.getenv("KIS_IS_DEMO", "true").lower() == "true"
APP_KEY   = os.getenv("KIS_APP_KEY", "")
APP_SECRET= os.getenv("KIS_APP_SECRET", "")
ACCOUNT_NO= os.getenv("KIS_ACCOUNT_NO", "")
CANO      = os.getenv("KIS_CANO") or (ACCOUNT_NO.replace("-", "")[:8] if ACCOUNT_NO else "")
ACNT_CD   = os.getenv("KIS_ACNT_PRDT_CD") or (ACCOUNT_NO.replace("-", "")[-2:] if ACCOUNT_NO else "")

BASE = (
    "https://openapivts.koreainvestment.com:29443" if IS_DEMO
    else "https://openapi.koreainvestment.com:9443"
)

DEFAULT_EXCHG = "NAS"

# ---------------------------------------------------------
# TR-ID / PATH 상수 (실제 반영)
# ---------------------------------------------------------
TRID_TOKEN = None
PATH_TOKEN = "/oauth2/token" if IS_DEMO else "/oauth2/tokenP"

TRID_PRICE = "HHDFS76200100"
PATH_PRICE = "/uapi/overseas-price/v1/quotations/inquire-asking-price"

TRID_BALANCE = {
    "real": "TTTS3012R"    # 실전 계좌
}
PATH_BALANCE = "/uapi/overseas-stock/v1/trading/inquire-balance"

TRID_ORDER = {"demo": "TTTT1002R", "real": "TTTT1002U"}
PATH_ORDER = "/uapi/overseas-stock/v1/trading/order"

TRID_CANCEL = {"demo": "TTTT1004R", "real": "TTTT1004U"}
PATH_CANCEL = "/uapi/overseas-stock/v1/trading/order-cancel"

TRID_ORDER_INQ = {"demo": "TTTS1001R", "real": "TTTS1001U"}
PATH_ORDER_INQ = "/uapi/overseas-stock/v1/trading/inquire-order"

PATH_UNFILLED = "/uapi/overseas-stock/v1/trading/inquire-unfilled-order"

# ---------------------------------------------------------
# 토큰 캐시 관리
# ---------------------------------------------------------
_TOKEN: Optional[str] = None
_TOKEN_EXPIRES_AT: int = 0

def _now() -> int:
    return int(time.time())

import json

TOKEN_FILE = "kis_token.json"

def save_token(token: str, expires_at: int):
    data = {"access_token": token, "expires_at": expires_at}
    with open(TOKEN_FILE, "w", encoding="utf-8") as f:
        json.dump(data, f)

def load_token():
    if not os.path.exists(TOKEN_FILE):
        return None, 0
    try:
        with open(TOKEN_FILE, "r", encoding="utf-8") as f:
            data = json.load(f)
            return data.get("access_token"), data.get("expires_at")
    except:
        return None, 0


def _get_token(force: bool = False) -> str:
    global _TOKEN, _TOKEN_EXPIRES_AT

    # 1) 파일에서 토큰 로드
    if not _TOKEN:
        saved_token, saved_exp = load_token()
        if saved_token and _now() < saved_exp - 120:
            _TOKEN = saved_token
            _TOKEN_EXPIRES_AT = saved_exp
            print("🔑 [KIS] 저장된 토큰 불러오기 성공")
            return _TOKEN

    # 2) 메모리 캐싱 체크
    if not force and _TOKEN and _now() < _TOKEN_EXPIRES_AT - 120:
        return _TOKEN

    # 3) 새로운 토큰 발급
    url = f"{BASE}{PATH_TOKEN}"
    headers = {"content-type": "application/json"}
    body = {
        "grant_type": "client_credentials",
        "appkey": APP_KEY,
        "appsecret": APP_SECRET,
    }

    res = requests.post(url, headers=headers, data=json.dumps(body))
    data = res.json()
    _TOKEN = data["access_token"]
    expires_in = int(float(data.get("expires_in", 86400)))
    _TOKEN_EXPIRES_AT = _now() + expires_in

    # 4) 파일에도 저장
    save_token(_TOKEN, _TOKEN_EXPIRES_AT)
    print(f"🔑 [KIS] 토큰 새로 발급 + 파일 저장 완료 (유효 {expires_in/3600:.1f}시간)")

    return _TOKEN






def _headers(tr_id: Optional[str] = None) -> Dict[str, str]:
    h = {
        "Content-Type": "application/json; charset=UTF-8",
        "authorization": f"Bearer {_get_token()}",
        "appkey": APP_KEY,
        "appsecret": APP_SECRET,
    }
    if tr_id:
        h["tr_id"] = tr_id
    return h

def _request(method: str, path: str, *, params=None, json=None, tr_id: Optional[str] = None):
    url = BASE + path
    headers = _headers(tr_id)
    return _send_request(method, url, headers=headers, params=params, data=json)


def _send_request(method, url, headers=None, params=None, data=None, retry=True):
    try:
        response = requests.request(method, url, headers=headers, params=params, data=data, timeout=10)
        response.raise_for_status()
        data = response.json()
    except requests.exceptions.RequestException as e:
        # ✅ 네트워크 오류 시 5초 후 재시도 1회
        if retry:
            print(f"⚠️ [KIS] 네트워크 예외 발생, 5초 후 재시도: {e}")
            time.sleep(5)
            return _send_request(method, url, headers=headers, params=params, data=data, retry=False)
        else:
            raise RuntimeError(f"[KIS] 네트워크 요청 실패: {e}")

    # ✅ 토큰 만료 감지
    msg_code = str(data.get("msg_cd", "")).upper()
    if msg_code in ("EGW00123", "EGW00115", "EGW00114") or "INVALID TOKEN" in str(data).upper():
        print("🔄 [KIS] 액세스 토큰 만료 감지 → 자동 재발급 시도")
        if retry:
            _get_token(force=True)
            headers["authorization"] = f"Bearer {_TOKEN}"
            return _send_request(method, url, headers=headers, params=params, data=data, retry=False)
        else:
            raise RuntimeError("❌ [KIS] 토큰 재발급 실패 (2회 연속)")

    return data



# ---------------------------------------------------------
# 유틸
# ---------------------------------------------------------
def _split_symbol(symbol: str):
    s = symbol.strip().upper()
    if "." in s:
        t, ex = s.split(".", 1)
        return t, ex
    return s, DEFAULT_EXCHG

def _ceil_price_to_cent(p: float) -> float:
    return math.floor(p * 100 + 0.5) / 100.0

def _round_qty_to_share(q: float) -> int:
    return int(math.floor(q))

# ---------------------------------------------------------
# 더미 함수
# ---------------------------------------------------------
def get_position_mode(): return False
def set_hedge_mode(mode: bool): return None
def set_leverage(symbol: str, leverage: int): return None

# ---------------------------------------------------------
# 시세 조회
# ---------------------------------------------------------
def get_current_ask_price(market: str) -> float:

    ticker, ex = _split_symbol(market)
    params = {
        "AUTH": "",
        "EXCD": ex,
        "SYMB": ticker
    }

    data = _request("GET", PATH_PRICE, params=params, tr_id=TRID_PRICE)

    # 033 API 구조: output1, output2, output3
    output1 = data.get("output1")
    if not output1:
        raise MarketClosedError(f"no output1 in response: {data}")

    # last(현재가)는 output1.last
    last_price = output1.get("last")
    if not last_price or last_price in ("", "0", None):
        raise MarketClosedError("market closed or no live price data")

    try:
        price = float(last_price)
        return price
    except Exception as e:
        print(f"❌ [DEBUG][PRICE] price parse 실패: {e}")
        raise MarketClosedError(f"quote parse failed: {data}")




# ---------------------------------------------------------
# 잔고 조회
# ---------------------------------------------------------
def get_accounts() -> Dict[str, Dict]:
    tr_id = TRID_BALANCE["real"]
    params = {
        "CANO": CANO,
        "ACNT_PRDT_CD": ACNT_CD,
        "OVRS_EXCG_CD": "NAS",  # 미국 전체
        "TR_CRCY_CD": "USD",
        "CTX_AREA_FK200": "",
        "CTX_AREA_NK200": "",
    }

    data = _request("GET", PATH_BALANCE, params=params, tr_id=tr_id)
    output = data.get("output1") or []

    holdings = {}

    for row in output:
        symbol = row.get("ovrs_pdno", "").strip().upper()
        qty    = float(row.get("ovrs_cblc_qty", "0") or 0)
        avg    = float(row.get("pchs_avg_pric", "0") or 0)

        if qty > 0:
            holdings[symbol] = {
                "balance": qty,
                "avg_buy_price": avg,
                "side": "LONG",
                "leverage": 1,
                "liquidation_price": 0.0,
            }

    return holdings


# ---------------------------------------------------------
# 주문
# ---------------------------------------------------------
def send_order(market: str, side: str, ord_type: str,
               unit_price: Optional[float] = None,
               volume: Optional[float] = None,
               **kwargs) -> Dict:
    ticker, ex = _split_symbol(market)

    ex = "NASD"
    # ✔ PDNO는 필수
    PDNO = market
    qty = str(_round_qty_to_share(volume))

    # ✔ 지정가 주문만 가능
    price = str(_ceil_price_to_cent(unit_price))

    # ==============================
    #   ❗ BUY / SELL 구분
    # ==============================
    if side.upper() == "BUY":
        tr_id = "VTTT1002U" if IS_DEMO else "TTTT1002U"
        payload = {
            "CANO": CANO,
            "ACNT_PRDT_CD": ACNT_CD,
            "OVRS_EXCG_CD": "NASD",
            "PDNO": PDNO,
            "ORD_QTY": str(qty),
            "OVRS_ORD_UNPR": str(price),
            "ORD_SVR_DVSN_CD": "0",
            "ORD_DVSN": "00",  # 지정가
        }
    elif side.upper() == "SELL":
        tr_id = "VTTT1001U" if IS_DEMO else "TTTT1006U"
        payload = {
            "CANO": CANO,
            "ACNT_PRDT_CD": ACNT_CD,
            "OVRS_EXCG_CD": "NASD",  # FIX
            "PDNO": PDNO,  # TQQQ
            "ORD_QTY": str(qty),
            "OVRS_ORD_UNPR": str(price),
            "SLL_TYPE": "00",  # FIX (매도 필수)
            "ORD_SVR_DVSN_CD": "0",
            "ORD_DVSN": "00",  # 지정가
        }
    else:
        raise ValueError(f"❌ side must be BUY or SELL. given={side}")

    # ==============================
    #   실제 요청
    # ==============================
    res = _request(
        "POST",
        PATH_ORDER,
        json=json.dumps(payload),
        tr_id=tr_id
    )

    out = res.get("output") or {}
    uuid = normalize_uuid(out.get("ODNO") or out.get("ord_no"))

    return {"uuid": uuid, "raw": res}



# ---------------------------------------------------------
# 주문 취소
# ---------------------------------------------------------
def cancel_orders_by_uuids(uuid_list: List[str], market: str) -> Dict:
    """
    해외주식 정정/취소 주문 API (문서 100% 일치 버전)
    RVSE_CNCL_DVSN_CD = 02 → 취소
    """
    ticker, ex = _split_symbol(market)
    ex = "NASD"  # 미국 고정

    tr_id = "VTTT1004U" if IS_DEMO else "TTTT1004U"
    path = "/uapi/overseas-stock/v1/trading/order-rvsecncl"

    success = []
    fail = []

    for odno in uuid_list:

        # ⭐⭐ 여기서 원주문번호를 반드시 10자리 문자열로 만들어줌 ⭐⭐
        pad_odno = normalize_uuid(odno).zfill(10)    # ← 핵심 포인트

        try:
            payload = {
                "CANO": CANO,
                "ACNT_PRDT_CD": ACNT_CD,
                "OVRS_EXCG_CD": ex,
                "PDNO": ticker,
                "ORGN_ODNO": pad_odno,     # ← 패딩된 값 넣기
                "RVSE_CNCL_DVSN_CD": "02",
                "ORD_QTY": "0",
                "OVRS_ORD_UNPR": "0",
                "ORD_SVR_DVSN_CD": "0"
            }

            res = _request(
                "POST",
                path,
                json=json.dumps(payload),
                tr_id=tr_id
            )
            success.append({"uuid": pad_odno, "raw": res})

        except Exception as e:
            fail.append({"uuid": pad_odno, "error": str(e)})

    return {"success": success, "failed": fail}



# ---------------------------------------------------------
# 주문 상태 조회
# ---------------------------------------------------------
KIS_DONE = {"체결", "filled", "00"}
KIS_CANCEL = {"취소", "cancel", "99"}

# ---------------------------------------------------------
# 해외주식 체결내역 조회 (filled)
# ---------------------------------------------------------
def _kis_get_filled_orders(market: str, start_dt: str, end_dt: str) -> Dict[str, dict]:
    """
    해외주식 체결내역 조회 API (inquire-ccnl)
    특정 기간 동안 체결된 주문들을 불러온다.
    반환: { 주문번호(ODNO): row }
    """
    tr_id = "VTTS3035R" if IS_DEMO else "TTTS3035R"
    path = "/uapi/overseas-stock/v1/trading/inquire-ccnl"

    ticker, ex = _split_symbol(market)

    params = {
        "CANO": CANO,
        "ACNT_PRDT_CD": ACNT_CD,
        "PDNO": ticker,            # 종목명만 (TQQQ)
        "ORD_STRT_DT": start_dt,   # YYYYMMDD (현지시간)
        "ORD_END_DT": end_dt,
        "SLL_BUY_DVSN": "00",      # 전체
        "CCLD_NCCS_DVSN": "01",    # 01 = 체결만
        "OVRS_EXCG_CD": ex,        # NASD
        "SORT_SQN": "DS",          # 정순
        "ORD_DT": "",
        "ORD_GNO_BRNO": "",
        "ODNO": "",                # ※ 주문번호 검색 불가 → 반드시 ""
        "CTX_AREA_NK200": "",
        "CTX_AREA_FK200": "",
    }

    data = _request("GET", path, params=params, tr_id=tr_id)
    output = data.get("output") or []
    result = {}

    for row in output:
        odno = normalize_uuid(row.get("odno"))
        if odno:
            result[odno] = row

    return result


# ---------------------------------------------------------
# 해외주식 미체결 주문 조회 (wait)
# ---------------------------------------------------------
def _kis_get_unfilled_orders(market: str) -> Dict[str, dict]:
    """
    해외주식 미체결 주문 조회 (실전 전용)
    문서: 해외주식 미체결내역 v1_해외주식-005
    TR-ID: TTTS3018R
    URL  : /uapi/overseas-stock/v1/trading/inquire-nccs

    return:
        { 주문번호(ODNO): row_dict }
    """

    symbol = market.upper()

    params = {
        "CANO": CANO,
        "ACNT_PRDT_CD": ACNT_CD,
        "OVRS_EXCG_CD": "NASD",   # 미국 전체
        "SORT_SQN": "DS",
        "CTX_AREA_FK200": "",
        "CTX_AREA_NK200": "",
    }

    data = _request(
        "GET",
        "/uapi/overseas-stock/v1/trading/inquire-nccs",
        params=params,
        tr_id="TTTS3018R"
    )

    output = data.get("output") or []
    result = {}

    for row in output:
        # 특정 종목만 추리기 (TQQQ 등)
        if row.get("pdno", "").upper() != symbol:
            continue

        odno = normalize_uuid(row.get("odno"))
        if odno:
            result[odno] = row

    return result


def get_order_results_by_uuids(uuid_list: List[str], market: str) -> Dict[str, str]:
    """
    해외주식 주문 상태 조회
    - filled  → done
    - unfilled → wait
    - else     → cancel
    """

    # 정규화된 uuid_list 준비
    norm_uuid_list = [normalize_uuid(u) for u in uuid_list]

    today = datetime.now().strftime("%Y%m%d")
    start_dt = (datetime.now() - timedelta(days=1)).strftime("%Y%m%d")

    filled_raw = _kis_get_filled_orders(market, start_dt, today)
    filled_map = {normalize_uuid(k): v for k, v in filled_raw.items()}

    unfilled_raw = _kis_get_unfilled_orders(market)
    unfilled_map = {normalize_uuid(k): v for k, v in unfilled_raw.items()}

    result = {}

    # 최종 판단
    for u in norm_uuid_list:
        if u in filled_map:
            result[u] = "done"
        elif u in unfilled_map:
            result[u] = "wait"
        else:
            result[u] = "cancel"

    return result



# ---------------------------------------------------------
# 정정(취소 후 신규)
# ---------------------------------------------------------
def cancel_and_new_order(prev_order_uuid: str, market: str, price: float, quantity: float, side: str, **kwargs) -> Dict:
    cancel_orders_by_uuids([prev_order_uuid], market)
    res = send_order(market=market, side=side, ord_type="limit", unit_price=price, volume=quantity)
    return {"new_order_uuid": res.get("uuid"), "raw": res}

def get_position_mode(): return False
def set_hedge_mode(mode: bool): return None
def set_leverage(symbol: str, leverage: int): return None
# api/kis_usstocks.py

# ====================================================
# 🇺🇸 미국장 개장 여부 조회 함수
# ====================================================

from datetime import datetime
import pytz
# ---------------------------------------------------------
# 🇺🇸 미국 휴장일 조회 (TR: CTCA0907R)
# ---------------------------------------------------------

def get_us_holidays(exchange: str = "NASD") -> Dict:
    """
    한국투자증권 API를 통해 해외(미국) 증시 휴장일을 조회합니다.
    TR: CTCA0907R
    """
    tr_id = "CTCA0907R"
    path = "/uapi/overseas-stock/v1/trading/holiday"
    params = {"EXCD": exchange}  # NASD, NYSE 등
    try:
        data = _request("GET", path, params=params, tr_id=tr_id)
        return data
    except Exception as e:
        raise RuntimeError(f"get_us_holidays() 호출 실패: {e}")

from datetime import datetime, date
import pytz
from api.kis_usstocks import get_us_holidays, _request, PATH_PRICE, TRID_PRICE

from datetime import datetime
import pytz
from api.kis_usstocks import _request

# 해외주식 현재가 상세 API
PATH_PRICE_DETAIL = "/uapi/overseas-price/v1/quotations/price-detail"
TRID_PRICE_DETAIL = "HHDFS76200200"

def is_us_market_open(symbol="AAPL", exchange="NAS"):
    """
    해외주식 현재가 상세 API(HHDFS76200200)를 기반으로
    미국 시장 개장 여부를 판별.
    프리/정규/애프터 시간 모두 조회 가능.
    """
    ny_tz = pytz.timezone("America/New_York")
    now_ny = datetime.now(ny_tz)

    # 미국 휴장일 판단은 별도 API로도 가능하지만
    # price-detail API 자체로도 충분함.

    params = {
        "AUTH": "",
        "EXCD": exchange,
        "SYMB": symbol
    }

    try:
        data = _request(
            "GET",
            PATH_PRICE_DETAIL,
            params=params,
            tr_id=TRID_PRICE_DETAIL
        )
    except Exception as e:
        print(f"[is_us_market_open] API 조회 실패 → 시장 닫힘으로 간주: {e}")
        return False

    output = data.get("output") or {}

    # 현재가
    last = output.get("last")
    open_price = output.get("open")
    volume = output.get("tvol")  # 거래량

    # 장이 완전히 닫혀 있으면 last/volume이 공백 또는 0
    if not last or last in ("", "0", 0, None):
        print("🔴 last 없음 또는 0 → 시장 비개장")
        return False

    # 프리/애프터에서 거래량이 거의 없을 수도 있으나 last는 존재함
    try:
        last_f = float(last)
    except:
        print("🔴 last 파싱 불가 → 시장 비개장")
        return False

    # 정상적인 가격이 들어오면 개장으로 판단
    print(f"🟢 미국 시장 개장 감지 (last={last_f})")
    return True


def get_algo_filled_details(odno: str, order_date: str) -> Dict:
    """
    해외주식 지정가(TWAP/VWAP) 체결내역 조회 API
    TR: TTTS6059R  (모의투자 미지원)
    Endpoint: /uapi/overseas-stock/v1/trading/inquire-algo-ccnl

    Params:
        odno        : 주문번호 (내부 저장된 uuid, ex: '31161743')
        order_date  : 주문일자 YYYYMMDD
    """
    tr_id = "TTTS6059R"  # 실전 전용
    path = "/uapi/overseas-stock/v1/trading/inquire-algo-ccnl"

    # KIS로 보낼 때는 10자리 padding이 필수
    pad_odno = normalize_uuid(odno).zfill(10)

    params = {
        "CANO": CANO,
        "ACNT_PRDT_CD": ACNT_CD,
        "ORD_DT": order_date,      # YYYYMMDD (예: '20250115')
        "ORD_GNO_BRNO": "",        # 선택
        "ODNO": pad_odno,          # ⭐ 패딩된 주문번호
        "TTLZ_ICLD_YN": "",
        "CTX_AREA_NK200": "",
        "CTX_AREA_FK200": "",
    }

    data = _request(
        "GET",
        path,
        params=params,
        tr_id=tr_id
    )

    return data
