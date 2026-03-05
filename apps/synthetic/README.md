# Synthetic

AI 학습용 synthetic 사용자/상호작용/implicit feedback 생성 전용 영역입니다.

## Scripts
- `generate_users.py`: synthetic 사용자 생성
- `generate_interactions.py`: synthetic 상호작용 생성
- `implicit_feedback.py`: 상호작용/이벤트를 implicit feedback 스키마로 변환

## Example
```bash
python3 apps/synthetic/generate_users.py --output output/synthetic/synthetic_users.csv
python3 apps/synthetic/generate_interactions.py \
  --users output/synthetic/synthetic_users.csv \
  --output output/synthetic/synthetic_interactions.csv
python3 apps/synthetic/implicit_feedback.py \
  --input output/synthetic/synthetic_interactions.csv \
  --input-type synthetic \
  --real-feedback-input implicit_feedback.csv \
  --target-total-with-real 10000 \
  --output output/synthetic/implicit_feedback_from_synthetic.csv
```

## Rule
- seeder 스크립트/데이터를 참조하지 않습니다.
