# Seeder

실데이터 수집/정제/DML 생성 전용 영역입니다.

## Scripts
- `pipeline.py`: 좌표 기반 수집 + DML 생성
- `generate_load_test_sql.py`: 부하테스트용 대량 시드 SQL 생성

## Example
```bash
python3 apps/seeder/pipeline.py --lat 37.402052 --lng 127.107058 \
  --out output/seeder/dml_output.sql

python3 apps/seeder/generate_load_test_sql.py \
  --restaurant-count 5000 \
  --out restaurant_load_test_seed.sql

python3 apps/seeder/generate_load_test_sql.py \
  --profile local-dense \
  --restaurant-count 20000 \
  --local-center-lat 37.402052 \
  --local-center-lng 127.107058 \
  --local-center-sido 경기도 \
  --local-center-sigungu "성남시 분당구" \
  --local-center-eupmyeondong 삼평동 \
  --out output/seeder/local_dense_restaurant_seed.sql
```

## Rule
- synthetic 스크립트/데이터를 참조하지 않습니다.
