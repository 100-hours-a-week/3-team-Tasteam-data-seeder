# Seeder

실데이터 수집/정제/DML 생성 전용 영역입니다.

## Scripts
- `pipeline.py`: 좌표 기반 수집 + DML 생성
- `menu_crawling.py`: 메뉴 크롤링
- `build_dml_from_existing.py`: 기존 데이터 기반 DML 생성
- `res_basic.py`: 보조 스크립트

## Example
```bash
python3 apps/seeder/pipeline.py --lat 37.402052 --lng 127.107058 \
  --out output/seeder/dml_output.sql
```

## Rule
- synthetic 스크립트/데이터를 참조하지 않습니다.
