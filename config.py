import os
from dotenv import load_dotenv

# .env 파일 불러오기
load_dotenv()

ACCESS_KEY = os.getenv("UPBIT_OPEN_API_ACCESS_KEY")
SECRET_KEY = os.getenv("UPBIT_OPEN_API_SECRET_KEY")
SERVER_URL = os.getenv("UPBIT_OPEN_API_SERVER_URL", "https://api.upbit.com")

# 옵션: 환경변수가 없을 때 경고
if not ACCESS_KEY or not SECRET_KEY:
    raise ValueError("API 키가 설정되지 않았습니다. .env 파일을 확인하세요.")
