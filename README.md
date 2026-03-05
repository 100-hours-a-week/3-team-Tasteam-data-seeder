# tasteam-data-seeder

이 저장소는 다음 두 영역을 엄격히 분리합니다.

- `apps/seeder`: 음식점/메뉴 크롤링 및 DML 생성
- `apps/synthetic`: AI 학습용 synthetic 데이터 생성

## Separation Rule
- `seeder`와 `synthetic`는 서로 import/호출하지 않습니다.
- 입력/출력 데이터도 섞지 않습니다.
- 운영 시 경로를 분리하세요.
  - `output/seeder/*`
  - `output/synthetic/*`

## Directory
```text
apps/
  seeder/
    pipeline.py
    menu_crawling.py
    build_dml_from_existing.py
    res_basic.py
    README.md
  synthetic/
    generate_users.py
    generate_interactions.py
    implicit_feedback.py
    README.md
```

## Quick Start
Seeder:
```bash
python3 apps/seeder/pipeline.py --lat 37.402052 --lng 127.107058 \
  --out output/seeder/dml_output.sql
```

Synthetic:
```bash
python3 apps/synthetic/generate_users.py --output output/synthetic/synthetic_users.csv
python3 apps/synthetic/generate_interactions.py \
  --users output/synthetic/synthetic_users.csv \
  --output output/synthetic/synthetic_interactions.csv
python3 apps/synthetic/implicit_feedback.py \
  --input output/synthetic/synthetic_interactions.csv \
  --input-type synthetic \
  --output output/synthetic/implicit_feedback_from_synthetic.csv
```
