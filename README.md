# Tasteam 파이프라인

`lat/lng`를 입력해 음식점 데이터를 수집하고 DML SQL을 생성하는 스크립트입니다.  
기본 모드는 API 모드이며, 필요 시 Local 모드로도 실행할 수 있습니다.

## 초기 환경설정
### 1) 가상환경 생성 및 활성화
```bash
python3 -m venv .venv
source .venv/bin/activate
```

### 2) 패키지 설치
```bash
python -m pip install -U pip
python -m pip install selenium
```

### 3) API 키 설정 (API 모드 사용 시 필수)
```bash
export GCP_API_KEY=YOUR_KEY
```

`GCP_API_KEY` 또는 `API_KEY` 중 하나가 설정되어 있으면 됩니다.

### 4) 브라우저 준비 (네이버 메뉴 크롤링 사용 시)
- Chrome 설치 필요
- Selenium 실행 환경이 없으면 `--no-naver` 옵션으로 실행

## CLI 사용법
### 1) API 모드 실행 (기본)
```bash
export GCP_API_KEY=YOUR_KEY
python3 pipeline.py --lat 37.402052 --lng 127.107058 --out dml_output.sql
```

### 2) API 모드 + 리포트 저장
```bash
export GCP_API_KEY=YOUR_KEY
python3 pipeline.py --lat 37.402052 --lng 127.107058 \
  --out dml_output.sql --report report.json
```

### 3) API 모드 (네이버 크롤링 끄기)
```bash
export GCP_API_KEY=YOUR_KEY
python3 pipeline.py --lat 37.402052 --lng 127.107058 \
  --out dml_output.sql --no-naver
```

### 4) Local 모드 실행 (이미 저장된 JSON 사용)
```bash
python3 pipeline.py --mode local --lat 37.402052 --lng 127.107058 \
  --out dml_output.sql
```

## 모드별 사용 설명
### API 모드 (`--mode api`, 기본)
- 언제 사용하나: 특정 좌표 주변 식당을 새로 수집할 때
- 필수 조건: `GCP_API_KEY`(또는 `API_KEY`) 환경변수
- 입력 데이터: 없음 (스크립트가 Google Places API에서 직접 조회)
- 실행 흐름:
  1. `lat/lng` + `radius`로 주변 음식점 조회
  2. 각 음식점 메뉴를 네이버 크롤링으로 보강 (기본 ON)
  3. DML 파일 생성
- 캐시:
  - 장소 조회 결과: `cache/places_{lat}_{lng}_{radius}_{rank}_{type}_{lang}.json`
  - 메뉴 조회 결과: `cache/menu/{store_name}.json`
- 예시:
```bash
export GCP_API_KEY=YOUR_KEY
python3 pipeline.py --mode api --lat 37.402052 --lng 127.107058 \
  --radius 500 --max 20 --out dml_output.sql
```

### Local 모드 (`--mode local`)
- 언제 사용하나: 이미 수집해 둔 JSON으로 재생성/재가공할 때
- 필수 조건: 장소 JSON + 메뉴 JSON 파일이 미리 준비되어 있어야 함
- 입력 데이터:
  - 장소 JSON: `--places-glob` 패턴으로 읽음 (기본: `ktb_res_*.json`)
  - 메뉴 JSON 디렉터리: `--menus-dir` 하위 `*.json` (기본: `uploaded`)
  - 선택: `--override-json` 파일로 장소 정보 보정
- 파일 위치 예시:
  - 장소 JSON: `./data/places/ktb_res_pangyo.json`
  - 메뉴 JSON: `./menus/문막집_menu.json`, `./menus/조이포_menu.json`
- 실행 흐름:
  1. 장소 JSON을 읽어 이름 기준 매핑 생성
  2. 메뉴 JSON의 `store_name`과 장소를 매칭
  3. 매칭된 항목만 DML로 출력
- 예시 (현재 저장소 구조 기준):
```bash
python3 pipeline.py --mode local --lat 37.402052 --lng 127.107058 \
  --menus-dir menus \
  --places-glob "data/places/ktb_res_*.json" \
  --out dml_output.sql
```

## 출력 결과
- DML 파일: `--out` (기본값 `dml_output.sql`)
- 리포트 JSON: `--report report.json` (선택)

진행 로그 예시:
```text
[1/20] appended place=규카츠정 판교점
[2/20] skipped (menu_not_found) place=깡우동 판교유스페이스점
...
```

## 주요 옵션
공통 옵션:
- `--lat`, `--lng` (필수)
- `--out` 출력 SQL 경로
- `--start-id` restaurant 시작 ID (기본 `9000`)
- `--report` 리포트 JSON 경로
- `--dry-run` 파일 저장 없이 실행

API 모드 옵션:
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

Local 모드 옵션:
- `--mode local`
- `--menus-dir` (기본 `uploaded`)
- `--places-glob` (기본 `ktb_res_*.json`)
- `--override-json` (선택, 기본 None)

## 캐시 구조 (API 모드)
```text
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
