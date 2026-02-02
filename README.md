# Tasteam 파이프라인

`lat/lng`를 입력하면 DML 파일을 생성하는 단일 스크립트입니다.  
기본 모드는 Google Places API + 네이버 지도 메뉴 크롤링(Selenium)입니다.

## 요구 사항
- Python 3.9+
- Google Places API 키
  - `GCP_API_KEY` 또는 `API_KEY` 환경변수 설정
- Selenium + Chrome (네이버 메뉴 크롤링 시)
  - Selenium 환경이 없으면 `--no-naver` 사용

## 빠른 시작
### 1) API 모드 (기본)
```
export GCP_API_KEY=YOUR_KEY
python3 pipeline.py --lat 37.402052 --lng 127.107058 --out dml_output.sql
```

### 2) API 모드 + 리포트 저장
```
export GCP_API_KEY=YOUR_KEY
python3 pipeline.py --lat 37.402052 --lng 127.107058 \
  --out dml_output.sql --report report.json
```

### 3) API 모드 (네이버 크롤링 끄기)
```
export GCP_API_KEY=YOUR_KEY
python3 pipeline.py --lat 37.402052 --lng 127.107058 --no-naver
```

### 4) Local 모드 (이미 저장된 JSON 사용)
```
python3 pipeline.py --mode local --lat 37.402052 --lng 127.107058 \
  --out dml_output.sql
```

## 출력
- DML 파일: `--out` (기본값 `dml_output.sql`)
- 리포트 JSON: `--report report.json` (선택)

진행 로그 예시:
```
[1/20] appended place=규카츠정 판교점
[2/20] skipped (menu_not_found) place=깡우동 판교유스페이스점
...
```

## 주요 옵션
공통:
- `--lat`, `--lng` (필수)
- `--out` 출력 SQL 경로
- `--start-id` restaurant 시작 ID (기본 `9000`)
- `--report` 리포트 JSON 경로
- `--dry-run` 파일 저장 없이 실행

API 모드:
- `--mode api` (기본)
- `--radius` (기본 `500`)
- `--rank` (`DISTANCE` 또는 `POPULARITY`)
- `--lang` (기본 `ko`)
- `--type` (기본 `restaurant`)
- `--max` (기본 `20`)
- `--cache-dir` (기본 `cache`)
- `--use-naver` (기본 ON)
- `--no-naver` (네이버 크롤링 끄기)
- `--sleep` (요청 간 대기, 기본 `0.2`)

Local 모드:
- `--mode local`
- `--menus-dir` (기본 `uploaded`)
- `--places-glob` (기본 `ktb_res_*.json`)
- `--override-json` (선택, 기본 None)

## 캐시 구조 (API 모드)
```
cache/
  places_{lat}_{lng}_{radius}_{rank}_{type}_{lang}.json
  menu/
    {store_name}.json
```

## 참고
- food_category 관련 INSERT는 생성하지 않습니다.
- 레스토랑 단위로 아래 순서로 DML이 출력됩니다:
  `restaurant → address → schedule → menu_category → menu`
- 메뉴 항목에서 UI 노이즈와 `(메뉴명 없음)`은 필터링됩니다.
