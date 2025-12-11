# api/auth.py

import uuid
import jwt  # PyJWT
import config


def generate_jwt_token(query: dict = None) -> str:
    print("[auth.py] generate_jwt_token() 호출됨")

    payload = {
        'access_key': config.ACCESS_KEY,
        'nonce': str(uuid.uuid4())
    }

    if query:
        import hashlib
        from urllib.parse import urlencode, unquote

        # ⚠️ 반드시 unquote 처리 포함해야 함
        query_string = unquote(urlencode(query, doseq=True))
        m = hashlib.sha512()
        m.update(query_string.encode())
        payload['query_hash'] = m.hexdigest()
        payload['query_hash_alg'] = 'SHA512'

    jwt_token = jwt.encode(payload, config.SECRET_KEY)
    print("[auth.py] JWT 토큰 생성 완료")
    return f'Bearer {jwt_token}'
