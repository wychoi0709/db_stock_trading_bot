# api/db_usstocks.py

import os
import time
import json
import requests
from dotenv import load_dotenv

load_dotenv()

# ==========================================
# 환경 변수
# ==========================================
APP_KEY = os.getenv("DB_APP_KEY", "")
APP_SECRET = os.getenv("DB_APP_SECRET", "")

BASE = "https://openapi.dbsec.co.kr:8443"
PATH_TOKEN = "/oauth2/token"

TOKEN_FILE = "db_token.json"

# 메모리 토큰 캐싱
_TOKEN = None
_TOKEN_EXPIRES_AT = 0


# ==========================================
# 내부 유틸
# ==========================================
def _now():
    return int(time.time())


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


# ==========================================
# ⭐ KIS와 멀티 호환을 위한 핵심 함수
#     이름은 반드시 _get_token()
# ==========================================
def _get_token(force: bool = False) -> str:
    """
    DB증권 접근 토큰 발급 (KIS와 동일한 함수명)
    - 파일 캐싱
    - 메모리 캐싱
    - 자동 재발급
    - 프로젝트 전체 호환
    """

    global _TOKEN, _TOKEN_EXPIRES_AT

    # ----------------------------
    # 1) 파일에서 토큰 읽기
    # ----------------------------
    if not _TOKEN:
        saved_token, saved_exp = load_token()
        if saved_token and _now() < saved_exp - 120:
            _TOKEN = saved_token
            _TOKEN_EXPIRES_AT = saved_exp
            print("🔑 [DB] 저장된 토큰 사용")
            return _TOKEN

    # ----------------------------
    # 2) 메모리 토큰 유효 → 그대로 사용
    # ----------------------------
    if not force and _TOKEN and _now() < _TOKEN_EXPIRES_AT - 120:
        return _TOKEN

    # ----------------------------
    # 3) DB증권 토큰 신규 발급
    # ----------------------------
    url = f"{BASE}{PATH_TOKEN}"

    headers = {
        "Content-Type": "application/x-www-form-urlencoded"
    }

    body = {
        "grant_type": "client_credentials",
        "appkey": APP_KEY,
        "appsecretkey": APP_SECRET,
        "scope": "oob"
    }

    try:
        res = requests.post(url, headers=headers, data=body)
        res.raise_for_status()
        data = res.json()
        time.sleep(0.2)
    except Exception as e:
        raise RuntimeError(f"❌ [DB] 접근토큰 발급 실패: {e}")

    # JSON 응답 처리
    token = data.get("access_token")
    expires_in = int(data.get("expires_in", 86400))

    if not token:
        raise RuntimeError(f"❌ [DB] access_token 없음: {data}")

    # 유효기간 계산
    expires_at = _now() + expires_in

    # 메모리 저장
    _TOKEN = token
    _TOKEN_EXPIRES_AT = expires_at

    # 파일 저장
    save_token(token, expires_at)

    print(f"🔑 [DB] 새 토큰 발급 완료 (유효 {expires_in/3600:.1f}시간)")

    return token


# ================================================
# 🇺🇸 DB증권 해외주식 잔고 조회
# 함수명은 반드시 get_accounts 유지 (KIS 호환)
# ================================================

import requests
import json
import os

BASE = "https://openapi.dbsec.co.kr:8443"
PATH_BALANCE = "/api/v1/trading/overseas-stock/inquiry/balance-margin"

def get_accounts() -> dict:
    """
    DB증권 해외주식 잔고 조회
    - 반환값은 기존 KIS 구조와 동일하게 매핑
    """

    url = BASE + PATH_BALANCE

    headers = {
        "Content-Type": "application/json; charset=utf-8",
        "authorization": f"Bearer {_get_token()}",
        "cont_yn": "N",
        "cont_key": "",
    }

    body = {
        "In": {
            "WonFcurrTpCode": "2",
            "TrxTpCode": "2",
            "CmsnTpCode": "2",
            "DpntBalTpCode": "1"
        }
    }

    try:
        res = requests.post(url, headers=headers, data=json.dumps(body))
        res.raise_for_status()
        data = res.json()
        time.sleep(0.2)

    except Exception as e:
        raise RuntimeError(f"❌ [DB] 해외주식 잔고 조회 실패: {e}")

    out2 = data.get("Out2") or []

    holdings = {}

    for row in out2:
        symbol = row.get("SymCode", "").strip().upper()

        # KIS의 ovrs_cblc_qty → DB의 AstkExecBaseQty
        qty = float(row.get("AstkExecBaseQty", "0") or 0)

        # KIS의 pchs_avg_pric → DB의 AstkAvrPchsPrc
        avg_price = float(row.get("AstkAvrPchsPrc", "0") or 0)

        if qty > 0:
            holdings[symbol] = {
                "balance": qty,
                "avg_buy_price": avg_price,
                "side": "LONG",
                "leverage": 1,
                "liquidation_price": 0.0,
            }

    return holdings

# ================================================
# 🇺🇸 DB증권 해외주식 현재가 조회 (Last Price)
# 함수명: get_current_last_price (신규)
# ================================================
PATH_PRICE = "/api/v1/quote/overseas-stock/inquiry/price"

def get_current_last_price(market: str, market_code: str) -> float:
    """
    DB증권 해외주식 현재가(최근 체결가) 조회
    - Prpr 필드 사용
    - ask/bid가 튈 때도 체결가 기준으로 안정적 로직 유지
    """
    symbol = market.strip().upper()

    url = BASE + PATH_PRICE

    headers = {
        "Content-Type": "application/json; charset=utf-8",
        "authorization": f"Bearer {_get_token()}",
        "cont_yn": "N",
        "cont_key": "",
    }

    body = {
        "In": {
            "InputIscd1": symbol,
            "InputCondMrktDivCode": market_code,
        }
    }

    try:
        res = requests.post(url, headers=headers, data=json.dumps(body))
        res.raise_for_status()
        data = res.json()
        time.sleep(0.2)
    except Exception as e:
        raise RuntimeError(f"❌ [DB] 해외주식 현재가 조회 실패: {e}")

    out = data.get("Out") or {}
    last = out.get("Prpr")  # 최근 체결가

    if not last or last in ("", "0", 0, None):
        raise RuntimeError(f"❌ [DB] Prpr(최근 체결가) 없음 → 장마감 또는 비정상 응답: {data}")

    try:
        return float(last)
    except:
        raise RuntimeError(f"❌ [DB] 체결가 파싱 실패: {last}")


# ================================================
# 🇺🇸 DB증권 해외주식 호가조회
# 함수명은 반드시 get_current_ask_price 유지 (KIS 호환)
# ================================================

PATH_ORDERBOOK = "/api/v1/quote/overseas-stock/inquiry/orderbook"

def get_current_ask_price(market: str, market_code: str) -> float:
    """
    DB증권 해외주식 호가조회 (KIS 동일 함수명)
    - market: "TQQQ", "AAPL" 등
    - 매도호가1(Askp1)을 현재가로 사용
    """

    symbol = market.strip().upper()

    # 심볼 → 거래시장 분류 필요
    # NYSE = FY
    # NASDAQ = FN
    # AMEX = FA
    # 기본은 나스닥(FN)로 설정 (KIS 기본 NAS)
    url = BASE + PATH_ORDERBOOK

    headers = {
        "Content-Type": "application/json; charset=utf-8",
        "authorization": f"Bearer {_get_token()}",
        "cont_yn": "N",
        "cont_key": "",
    }

    body = {
        "In": {
            "InputCondMrktDivCode": market_code,
            "InputIscd1": symbol,
        }
    }

    try:
        res = requests.post(url, headers=headers, data=json.dumps(body))
        res.raise_for_status()
        data = res.json()
        time.sleep(0.2)

    except Exception as e:
        raise RuntimeError(f"❌ [DB] 해외주식 호가 조회 실패: {e}")

    out = data.get("Out") or {}

    # Askp1 = 매도호가1
    ask = out.get("Askp1")

    if not ask or ask in ("", "0", 0, None):
        raise RuntimeError(f"❌ [DB] Askp1(매도호가) 없음 → 장마감 또는 비정상 응답: {data}")

    try:
        price = float(ask)
        return price
    except:
        raise RuntimeError(f"❌ [DB] 가격 파싱 실패: {ask}")


# ================================================
# 🇺🇸 DB증권 해외주식 호가조회 (매수호가 기반)
# 함수명: get_current_bid_price
# ================================================

PATH_ORDERBOOK = "/api/v1/quote/overseas-stock/inquiry/orderbook"

def get_current_bid_price(market: str, market_code: str) -> float:
    """
    DB증권 해외주식 호가조회
    - Bidp1(매수호가1)을 현재가로 사용
    - 시장 참여자들이 실제로 사려는 가격을 기준으로 판단하기 위함
    """

    symbol = market.strip().upper()
    url = BASE + PATH_ORDERBOOK

    headers = {
        "Content-Type": "application/json; charset=utf-8",
        "authorization": f"Bearer {_get_token()}",
        "cont_yn": "N",
        "cont_key": "",
    }

    body = {
        "In": {
            "InputCondMrktDivCode": market_code,
            "InputIscd1": symbol,
        }
    }

    try:
        res = requests.post(url, headers=headers, data=json.dumps(body))
        res.raise_for_status()
        data = res.json()
        time.sleep(0.2)
    except Exception as e:
        raise RuntimeError(f"❌ [DB] 해외주식 호가 조회 실패: {e}")

    out = data.get("Out") or {}

    # Bidp1 = 매수호가1
    bid = out.get("Bidp1")

    if not bid or bid in ("", "0", 0, None):
        raise RuntimeError(f"❌ [DB] Bidp1(매수호가) 없음 → 장마감 또는 비정상 응답: {data}")

    try:
        return float(bid)
    except:
        raise RuntimeError(f"❌ [DB] 가격 파싱 실패: {bid}")


# ================================================
# 🇺🇸 DB증권 해외주식 주문
# 함수명: send_order (KIS와 완전히 동일)
# ================================================

PATH_ORDER = "/api/v1/trading/overseas-stock/order"


def send_order(market: str, side: str, ord_type: str,
               unit_price: float = None,
               volume: float = None,
               **kwargs) -> dict:
    symbol = market.strip().upper()

    # --------------------------
    # 매수/매도 구분
    # --------------------------
    if side.upper() == "BUY":
        bns_code = "2"
    elif side.upper() == "SELL":
        bns_code = "1"
    else:
        raise ValueError(f"❌ side must be BUY or SELL. given={side}")

    # --------------------------
    # 지정가/시장가 구분
    # --------------------------
    ord_type = ord_type.lower()

    if ord_type == "limit":
        price_code = "1"  # 지정가
        order_price = float(unit_price)

    elif ord_type == "market":
        price_code = "2"  # 시장가
        order_price = 0  # 시장가 주문은 가격=0

    else:
        raise ValueError(f"❌ 지원하지 않는 ord_type: {ord_type}")

    qty = float(volume)

    url = BASE + PATH_ORDER

    headers = {
        "Content-Type": "application/json; charset=utf-8",
        "authorization": f"Bearer {_get_token()}",
        "cont_yn": "N",
        "cont_key": "",
    }

    body = {
        "In": {
            "AstkIsuNo": symbol,
            "AstkBnsTpCode": bns_code,
            "AstkOrdprcPtnCode": price_code,  # 1=지정가, 2=시장가
            "AstkOrdCndiTpCode": "1",  # 일반
            "AstkOrdQty": qty,
            "AstkOrdPrc": order_price,
            "OrdTrdTpCode": "0",
            "OrgOrdNo": 0
        }
    }

    try:
        res = requests.post(url, headers=headers, data=json.dumps(body))
        res.raise_for_status()
        data = res.json()
        time.sleep(0.2)

    except Exception as e:
        raise RuntimeError(f"❌ [DB] 해외주식 주문 실패: {e}")

    out = data.get("Out") or {}
    uuid = str(out.get("OrdNo"))

    if not uuid or uuid == "None":
        raise RuntimeError(f"❌ [DB] 주문번호 없음: {data}")

    return {
        "uuid": uuid,
        "raw": data
    }


# ================================================
# 🇺🇸 DB증권 해외주식 주문취소
# 함수명: cancel_orders_by_uuids (KIS 호환)
# ================================================

def cancel_orders_by_uuids(uuid_list: list, market: str) -> dict:
    """
    DB증권 해외주식 주문 취소
    - uuid_list: ['14', '27', ...] 형태의 주문번호 리스트
    - market: 'TQQQ' (DB API에서 사실상 필요 없음)
    - KIS와 동일한 반환 구조 유지
    """

    url = BASE + PATH_ORDER  # 주문 API와 동일

    headers = {
        "Content-Type": "application/json; charset=utf-8",
        "authorization": f"Bearer {_get_token()}",
        "cont_yn": "N",
        "cont_key": "",
    }

    success_list = []
    fail_list = []

    for uuid in uuid_list:
        org_no = int(uuid)  # DB는 숫자 주문번호 그대로 사용

        body = {
            "In": {
                "AstkIsuNo": market.upper(),
                "AstkBnsTpCode": "1",     # 매도=1, 매수=2 (취소는 무관하지만 문서상 필수 → 매도로 설정)
                "AstkOrdprcPtnCode": "1", # 지정가코드 (취소 시 무시되지만 필수 항목임)
                "AstkOrdCndiTpCode": "1", # FAS 일반
                "AstkOrdQty": 0,          # 취소 시 0 고정
                "AstkOrdPrc": 0,          # 취소 시 0 고정
                "OrdTrdTpCode": "2",      # ⭐ 2 = 취소 주문
                "OrgOrdNo": org_no        # ⭐ 기존 주문번호
            }
        }

        try:
            res = requests.post(url, headers=headers, data=json.dumps(body))
            res.raise_for_status()
            data = res.json()
            time.sleep(0.2)

            if data.get("Out", {}).get("OrdNo") in (None, "", 0):
                fail_list.append({
                    "uuid": uuid,
                    "rsp_cd": data.get("rsp_cd"),
                    "rsp_msg": data.get("rsp_msg"),
                })
                continue

            success_list.append({"uuid": uuid, "raw": data})
        except Exception as e:
            fail_list.append({"uuid": uuid, "error": str(e)})

    return {
        "success": success_list,
        "failed": fail_list
    }


# ===========================================================
# 🇺🇸 DB증권 → KIS 호환 주문 상태 조회
# 반환: {uuid: "wait" | "done" | "cancel"}
# ===========================================================

PATH_EXECUTION = "/api/v1/trading/overseas-stock/inquiry/transaction-history"


def get_order_results_by_uuids(uuid_list: list, market: str) -> dict:
    """
    DB증권 체결/미체결 전체 내역 조회 + uuid 매칭
    CAZCQ00100 : 해외주식 체결/미체결 조회 API 사용
    """

    url = BASE + PATH_EXECUTION  # 동일 endpoint 사용 (CAZCQ00100)
    today = time.strftime("%Y%m%d")
    yesterday = time.strftime("%Y%m%d", time.localtime(time.time() - 86400))

    all_rows = []
    cont_yn = "N"
    cont_key = ""

    while True:
        headers = {
            "Content-Type": "application/json; charset=utf-8",
            "authorization": f"Bearer {_get_token()}",
            "cont_yn": cont_yn,
            "cont_key": cont_key,
        }

        body = {
            "In": {
                "QrySrtDt": yesterday,
                "QryEndDt": today,
                "AstkIsuNo": market.upper(),
                "AstkBnsTpCode": "0",   # 전체
                "OrdxctTpCode": "0",    # 체결 + 미체결 전체
                "StnlnTpCode": "1",
                "QryTpCode": "1",
                "OnlineYn": "0",
                "CvrgOrdYn": "0",
                "WonFcurrTpCode": "2",
            }
        }

        try:
            res = requests.post(url, headers=headers, data=json.dumps(body))
            res.raise_for_status()
            data = res.json()
            time.sleep(0.2)
        except Exception as e:
            raise RuntimeError(f"❌ DB 체결/미체결 조회 실패: {e}")

        rows = data.get("Out") or []
        all_rows.extend(rows)

        header_cont_yn = res.headers.get("cont_yn", "N")
        next_key = res.headers.get("cont_key", "")

        if header_cont_yn != "Y":
            break

        cont_yn = "Y"
        cont_key = next_key

    # ================================================
    # uuid → 주문 데이터 매핑
    # ================================================
    execution_map = {str(r["OrdNo"]): r for r in all_rows if r.get("OrdNo")}

    result = {}

    for uuid in uuid_list:
        u = str(uuid).strip()

        if u not in execution_map:
            result[u] = "wait"
            continue

        row = execution_map[u]
        stat = str(row.get("AstkOrdStatCode", "")).strip()

        qty = float(row.get("AstkOrdQty", 0))  # 총 주문량
        exec_qty = float(row.get("AstkExecQty", 0))  # 체결량
        rm_qty = float(row.get("AstkOrdRmqty", 0))  # 잔량

        # ================================
        # 상태 판별
        # ================================
        if stat == "7":
            result[u] = "done"
        elif stat == "6":
            result[u] = "cancel"
        else:
            # 체결 없음 + 잔량 있음 → 미체결
            if exec_qty == 0 and rm_qty > 0:
                result[u] = "wait"
            # 부분체결 → wait
            elif 0 < exec_qty < qty:
                result[u] = "wait"
            else:
                result[u] = "wait"

    return result


def get_all_open_buy_orders(market: str) -> dict:
    """
    market의 전체 주문(체결/미체결)을 조회한 뒤,
    미체결(wait) 상태의 uuid만 반환하는 함수.
    반환 예시: {"12345": "wait", "12346": "wait"}
    """

    url = BASE + PATH_EXECUTION
    today = time.strftime("%Y%m%d")
    yesterday = time.strftime("%Y%m%d", time.localtime(time.time() - 86400))

    all_rows = []
    cont_yn = "N"
    cont_key = ""

    while True:
        headers = {
            "Content-Type": "application/json; charset=utf-8",
            "authorization": f"Bearer {_get_token()}",
            "cont_yn": cont_yn,
            "cont_key": cont_key,
        }

        body = {
            "In": {
                "QrySrtDt": yesterday,
                "QryEndDt": today,
                "AstkIsuNo": market.upper(),  # 특정 종목만 조회
                "AstkBnsTpCode": "0",         # 전체
                "OrdxctTpCode": "0",          # 체결 + 미체결
                "StnlnTpCode": "1",
                "QryTpCode": "1",
                "OnlineYn": "0",
                "CvrgOrdYn": "0",
                "WonFcurrTpCode": "2",
            }
        }

        res = requests.post(url, headers=headers, data=json.dumps(body))
        time.sleep(0.2)
        res.raise_for_status()
        data = res.json()
        rows = data.get("Out") or []
        all_rows.extend(rows)

        if res.headers.get("cont_yn", "N") != "Y":
            break

        cont_yn = "Y"
        cont_key = res.headers.get("cont_key", "")

    # 주문 상태 매핑
    uuid_map = {}

    for row in all_rows:
        uuid = str(row.get("OrdNo", "")).strip()
        if not uuid:
            continue

        qty = float(row.get("AstkOrdQty", 0))
        exec_qty = float(row.get("AstkExecQty", 0))
        rm_qty = float(row.get("AstkOrdRmqty", 0))
        stat = str(row.get("AstkOrdStatCode", "")).strip()

        # 상태 계산
        if stat == "7":
            state = "done"
        elif stat == "6":
            state = "cancel"
        else:
            # 체결 없음 + 잔량 있음 → 미체결
            if exec_qty == 0 and rm_qty > 0:
                state = "wait"
            # 부분체결도 미체결로 간주
            elif 0 < exec_qty < qty:
                state = "wait"
            else:
                state = "wait"

        uuid_map[uuid] = state

    # wait 상태만 반환
    return {u: s for u, s in uuid_map.items() if s == "wait"}





# api/db_usstocks.py

_last_order_price = {}

def cancel_and_new_order(prev_order_uuid: str, market: str, price: float, quantity: float, side: str):
    """
    DB증권 정정 주문 안정화 버전
    - 취소 → 신규주문 간 최소 텀 2.5초 확보
    - 동일 가격으로 반복 정정 금지
    """

    # -------------------------------------------
    # 2) 동일 가격 정정 금지
    # -------------------------------------------
    last_price = _last_order_price.get(market)
    if last_price and abs(last_price - price) < 0.0000001:
        print(f"🚫 [cancel_and_new_order] 동일 가격 정정 차단 → {price}")
        return {"new_order_uuid": None, "raw": None}

    print(f"[cancel_and_new_order] 기존 주문 취소 → 신규 주문 실행 (market={market}, price={price})")

    # -------------------------------------------
    # 3) 취소 후 딜레이
    # -------------------------------------------
    cancel_result = cancel_orders_by_uuids([prev_order_uuid], market)
    if cancel_result.get("failed"):
        raise RuntimeError(f"❌ 기존 주문 취소 실패: {cancel_result}")

    time.sleep(2.0)  # 너무 짧으면 자전거래 의심 발생

    # -------------------------------------------
    # 4) 신규 주문
    # -------------------------------------------
    order_res = send_order(
        market=market,
        side=side.upper(),
        ord_type="limit",
        unit_price=price,
        volume=quantity
    )

    # 성공 시 기록 업데이트
    _last_order_price[market] = price

    return {
        "new_order_uuid": order_res.get("uuid"),
        "raw": order_res
    }



def is_us_market_open(market: str, exchange: str = "FN") -> bool:
    """
    DB증권 해외주식 호가조회 기반 미국 시장 개장 여부 체크.
    - 프리/정규/애프터 모두 Ask/Bid가 들어오므로 개장으로 판별 가능
    - Askp1 또는 Bidp1이 없으면 시장 비개장으로 판단
    """

    url = f"{BASE}/api/v1/quote/overseas-stock/inquiry/orderbook"

    headers = {
        "Content-Type": "application/json; charset=utf-8",
        "authorization": f"Bearer {_get_token()}",
        "cont_yn": "N",
        "cont_key": "",
    }

    body = {
        "In": {
            "InputIscd1": market.upper(),      # 예: TQQQ
            "InputCondMrktDivCode": exchange,  # FN=나스닥, FY=뉴욕, FA=아멕스
        }
    }

    try:
        res = requests.post(url, headers=headers, data=json.dumps(body))
        res.raise_for_status()
        data = res.json()
        time.sleep(0.2)

    except Exception as e:
        print(f"❌ [is_us_market_open] API 오류 → 시장 닫힘 간주: {e}")
        return False

    out = data.get("Out") or {}

    ask = out.get("Askp1")
    bid = out.get("Bidp1")

    # 값이 없거나 0이면 폐장
    try:
        ask_f = float(ask)
        bid_f = float(bid)
    except:
        return False

    # 정상적인 숫자 → 개장
    if ask_f > 0 or bid_f > 0:
        return True

    return False


# api/db_usstocks.py 안

def get_bid_ask(market: str, market_code: str) -> tuple[float, float]:
    symbol = market.strip().upper()
    url = BASE + PATH_ORDERBOOK

    headers = {
        "Content-Type": "application/json; charset=utf-8",
        "authorization": f"Bearer {_get_token()}",
        "cont_yn": "N",
        "cont_key": "",
    }

    body = {
        "In": {
            "InputCondMrktDivCode": market_code,
            "InputIscd1": symbol,
        }
    }

    res = requests.post(url, headers=headers, data=json.dumps(body))
    res.raise_for_status()
    data = res.json()
    time.sleep(0.2)

    out = data.get("Out") or {}
    bid = out.get("Bidp1")
    ask = out.get("Askp1")

    if not bid or not ask or bid in ("", "0", 0, None) or ask in ("", "0", 0, None):
        raise RuntimeError(f"❌ [DB] Bid/Ask 없음 → 비정상 응답: {data}")

    return float(bid), float(ask)


def is_spread_too_wide(market: str, market_code: str,
                       max_spread_pct: float = 0.04) -> tuple[bool, float, float, float]:
    """
    스프레드가 비정상적으로 큰지 판단
    - max_spread_pct: 0.05 → 5%
    반환: (너무넓음 여부, spread_pct, bid, ask)
    """
    print(f"\n[spread-check] ▶ {market} / market_code={market_code}")

    bid, ask = get_bid_ask(market, market_code)
    print(f" - bid: {bid}, ask: {ask}")

    if not bid or not ask or bid in ("", "0", 0, None) or ask in ("", "0", 0, None):
        print(" ❗ 비정상 호가응답 → 스프레드 체크 불가")
        return True, 1.0, bid, ask  # 비정상 응답 시 '너무 넓음' 처리로 방어

    mid = (bid + ask) / 2
    spread_pct = (ask - bid) / mid if mid > 0 else 1.0

    print(f" - mid price: {mid:.4f}")
    print(f" - spread: {ask - bid:.4f} ({spread_pct * 100:.2f}%)")
    print(f" - threshold: {max_spread_pct * 100:.2f}%")

    is_wide = spread_pct >= max_spread_pct

    if is_wide:
        print(" 🚫 스프레드 너무 큼 → 거래 중단")
    else:
        print(" 🟢 스프레드 정상 → 거래 가능")

    return is_wide, spread_pct, bid, ask

