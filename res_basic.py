"""Google Places API searchNearby 호출 후 ktb_res.json으로 저장"""
import os
import json
import urllib.request
import argparse

url = "https://places.googleapis.com/v1/places:searchNearby"

parser = argparse.ArgumentParser(description="Google Places searchNearby")
parser.add_argument("--lat", type=float, required=True, help="센터 위도")
parser.add_argument("--lng", type=float, required=True, help="센터 경도")
parser.add_argument("--radius", type=float, default=500.0, help="반경(m)")
parser.add_argument("--out", default="ktb_res.json", help="출력 JSON 파일명")
parser.add_argument("--rank", default="DISTANCE", choices=["DISTANCE", "POPULARITY"], help="정렬")
parser.add_argument("--lang", default="ko", help="언어 코드")
parser.add_argument("--type", default="restaurant", help="includedTypes 단일 값")
parser.add_argument("--max", dest="max_count", type=int, default=20, help="최대 결과 수(보통 20 제한)")
args = parser.parse_args()

payload = {
    "includedTypes": [args.type],
    "maxResultCount": args.max_count,
    "languageCode": args.lang,
    "rankPreference": args.rank,
    "locationRestriction": {
        "circle": {
            "center": {"latitude": args.lat, "longitude": args.lng},
            "radius": args.radius,
        }
    },
}
api_key = os.environ.get("GCP_API_KEY") or os.environ.get("API_KEY")
if not api_key:
    raise SystemExit("GCP_API_KEY 또는 API_KEY 환경 변수를 설정하세요.")

# res_basic_photo.md 기준: 음식점 기본정보(Pro SKU) + 사진(places.photos)
FIELD_MASK = (
    "places.displayName,places.id,places.formattedAddress,places.location,"
    "places.primaryType,places.types,places.googleMapsUri,places.businessStatus,"
    "places.shortFormattedAddress,places.plusCode,places.viewport,places.photos,"
    "places.currentOpeningHours,places.currentSecondaryOpeningHours,"
    "places.regularOpeningHours,places.regularSecondaryOpeningHours"
)

req = urllib.request.Request(
    url,
    data=json.dumps(payload).encode("utf-8"),
    headers={
        "Content-Type": "application/json",
        "X-Goog-Api-Key": api_key,
        "X-Goog-FieldMask": FIELD_MASK,
    },
    method="POST",
)
try:
    with urllib.request.urlopen(req) as resp:
        data = json.loads(resp.read().decode())
except urllib.error.HTTPError as e:
    body = e.read().decode() if e.fp else ""
    raise SystemExit(f"HTTP {e.code}: {e.reason}\n{body}")

with open(args.out, "w", encoding="utf-8") as f:
    json.dump(data, f, ensure_ascii=False, indent=2)

print(f"저장 완료: {args.out}")
next_token = data.get("nextPageToken")
if next_token:
    print(f"nextPageToken: {next_token}")
else:
    print("nextPageToken 없음")
