def normalize_uuid(value):
    """
    주문번호(ODNO) 정규화 표준 함수
    - float 형태 제거: 31161743.0 → 31161743
    - 앞자리 0 제거: 0031161743 → 31161743
    - int/float/str 모두 처리
    - None / "" / "nan" / "{}" 등 무효값은 ""
    """

    if value in [None, "", "None", "nan", "NaN", {}, "{}", "null"]:
        return ""

    s = str(value).strip()

    # 숫자만 추출 (문자 포함된 케이스 대응)
    digits = "".join(ch for ch in s if ch.isdigit())

    if digits == "":
        return ""

    # float 형태(뒤에 .0 붙은 형태) 자동 제거
    try:
        return str(int(float(digits)))
    except:
        return str(int(digits))
