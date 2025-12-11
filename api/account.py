# api/account.py

import requests
from api.auth import generate_jwt_token
import config


def get_accounts():
    print("[account.py] get_accounts() 실행됨")

    headers = {
        'Authorization': generate_jwt_token()
    }

    response = requests.get(f"{config.SERVER_URL}/v1/accounts", headers=headers)

    if response.status_code == 200:
        print("[account.py] 계좌 조회 성공")
        return response.json()
    else:
        print("[account.py] 계좌 조회 실패")
        raise Exception(f"[Upbit API] 계좌 조회 실패: {response.status_code} - {response.text}")
