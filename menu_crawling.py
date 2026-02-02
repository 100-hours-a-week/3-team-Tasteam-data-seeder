"""
네이버 지도 검색 → 상세 → 메뉴 텍스트 크롤링.

메뉴를 못 찾는 흔한 원인:
1. 네이버 지도가 HTML/클래스명을 자주 변경함 (_3ak_I, V1UmJ 등)
2. 상세창 기본 탭이 '정보'라서 '메뉴' 탭을 한 번 클릭해야 메뉴 영역이 로드됨
3. entryIframe 로드 전에 요소를 찾음 (대기 부족)
4. 해당 가게에 메뉴가 등록되지 않음
"""
from selenium import webdriver
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.common.by import By
from selenium.webdriver.common.keys import Keys
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.common.exceptions import NoSuchWindowException, WebDriverException
from urllib3.exceptions import MaxRetryError, NewConnectionError
import time
import re
import json

# Selenium 4: 경로 지정 시 Service 사용. 경로 없으면 Selenium Manager가 자동으로 chromedriver 사용
# macOS 예: Service("/usr/local/bin/chromedriver") 또는 Service("/opt/homebrew/bin/chromedriver")
driver = None


def init_driver():
    global driver
    if driver is None:
        driver = webdriver.Chrome()
    return driver


def restart_driver():
    global driver
    try:
        if driver is not None:
            driver.quit()
    except Exception:
        pass
    driver = None
    return init_driver()

def menu(data): # 메뉴 크롤링
    init_driver()
    wait = WebDriverWait(driver, 15)
    driver.get("https://map.naver.com/v5/search/"+data) # 검색창에 가게이름 입력
    time.sleep(3)
    driver.implicitly_wait(5)
    # iframes = driver.find_elements_by_css_selector('iframe') # 창에 있는 모든 iframe 출력
    # for iframe in iframes:
    #     print(iframe.get_attribute('id'))
    driver.switch_to.frame('searchIframe') #  검색하고나서 가게정보창이 바로 안뜨는 경우 고려해서 무조건 맨위에 가게 링크 클릭하게 설정
    driver.implicitly_wait(3)
    try:
        temp = driver.find_element(By.XPATH, '//*[@id="_pcmap_list_scroll_container"]/ul') # 메뉴표에 있는 텍스트 모두 들고옴(개발자 도구에서 그때그때 xpath 복사해서 들고오는게 좋다)
    except Exception:
        return -1
    driver.implicitly_wait(20) # selenium에서 가끔씩 태그 시간내에 못찾는 경우 때문에 일부러 길게 설정해놓음
    button = temp.find_elements(By.TAG_NAME, 'a')
    driver.implicitly_wait(20)
    if '이미지수' in button[0].text or button[0].text == '': # 가게 정보에 사진이 있는경우
        button[1].send_keys(Keys.ENTER) 
    else: # 사진이 없는 경우
        button[0].send_keys(Keys.ENTER)
    driver.implicitly_wait(3)
    time.sleep(3)
    driver.switch_to.default_content()  # frame 복귀
    # entryIframe 로드 대기 (메뉴 정보가 있는 상세 iframe)
    try:
        wait.until(EC.frame_to_be_available_and_switch_to_it("entryIframe"))
    except Exception:
        print("entryIframe 로드 실패 (상세창이 안 떴거나 검색 결과 없음)")
        return -1
    time.sleep(2)  # 상세 패널 내부 렌더링 대기

    # '메뉴' 탭이 있으면 클릭 (많은 가게가 기본이 '정보' 탭이라 메뉴 탭을 눌러야 함)
    try:
        menu_tabs = driver.find_elements(By.XPATH, "//*[contains(text(), '메뉴')]")
        for tab in menu_tabs:
            if tab.is_displayed() and tab.is_enabled():
                tab.click()
                time.sleep(2)
                break
    except Exception:
        pass

    # 네이버 지도는 클래스명을 자주 바꿈. 여러 후보를 모아서 실제 메뉴(가격 포함)가 있는 것을 골라냄
    MENU_CLASS_CANDIDATES = ['_3ak_I', 'V1UmJ', 'place_section_content', 'K0PDV', 'E2BNj']
    seen = set()
    all_candidates = []
    for cls in MENU_CLASS_CANDIDATES:
        for el in driver.find_elements(By.CLASS_NAME, cls):
            uid = id(el)
            if uid not in seen:
                seen.add(uid)
                all_candidates.append(el)
    if not all_candidates:
        for el in driver.find_elements(By.CSS_SELECTOR, "[class*='menu'], [class*='Menu'], [class*='content']"):
            if id(el) not in seen:
                all_candidates.append(el)
    if len(all_candidates) == 0:
        print("메뉴를 찾지 못했습니다. 원인: 1) 네이버 지도 HTML/클래스 변경 2) 해당 가게에 메뉴 미등록 3) '메뉴' 탭 미클릭")
        return -1

    # 카테고리 탭이 아닌, 실제 메뉴(가격 포함)가 있는 텍스트를 선택. 전체 메뉴 컨테이너가 더 많은 줄을 가지므로 줄 수를 크게 반영
    def menu_score(text):
        if not text or not text.strip():
            return -1
        score = 0
        if "원" in text:
            score += 10
        lines = [l.strip() for l in text.splitlines() if l.strip()]
        score += min(len(lines), 150)  # 전체 메뉴일수록 줄 수 많음 → 첫 메뉴 블록만 고르지 않도록 상한 올림
        if any(c.isdigit() for c in text) and "원" in text:
            score += 5
        return score

    def score_el(el):
        try:
            t = el.text or ""
            return (menu_score(t), len(t))
        except Exception:
            return (-1, 0)

    best = max(all_candidates, key=score_el)  # 점수 같으면 텍스트 더 긴 것(전체 메뉴 컨테이너) 선택

    # 스크롤 가능한 부모를 찾아 조금씩 천천히 끝까지 스크롤 (lazy load·렌더링 여유)
    SCROLL_STEP = 250   # 한 번에 내릴 픽셀
    SCROLL_PAUSE = 0.55  # 스크롤 간 대기(초)
    scrollable = None
    try:
        scrollable = driver.execute_script("""
        var el = arguments[0];
        while (el) {
            if (el.scrollHeight > el.clientHeight) {
                return el;
            }
            el = el.parentElement;
        }
        return null;
        """, best)
        if scrollable:
            last_top = -1
            for _ in range(80):
                now = driver.execute_script(
                    "var s = arguments[0]; s.scrollTop += arguments[1]; return s.scrollTop;",
                    scrollable, SCROLL_STEP
                )
                time.sleep(SCROLL_PAUSE)
                if now == last_top:
                    break
                last_top = now
                at_bottom = driver.execute_script(
                    "var s = arguments[0]; return s.scrollTop + s.clientHeight >= s.scrollHeight - 2;",
                    scrollable
                )
                if at_bottom:
                    break
            driver.execute_script("document.body.scrollTop = document.body.scrollHeight;")
            time.sleep(0.5)
    except Exception:
        pass

    # 스크롤한 컨테이너가 전체 메뉴를 담고 있음 → scrollable 텍스트를 우선 사용 (길면 전체 메뉴)
    time.sleep(0.5)
    def get_menu_text(el):
        try:
            t = driver.execute_script("return arguments[0].innerText || '';", el)
            return (t or '').strip()
        except Exception:
            return ''
    t_best = ''
    t_scroll = ''
    try:
        t_best = get_menu_text(best)
    except Exception:
        pass
    if scrollable:
        try:
            t_scroll = get_menu_text(scrollable)
        except Exception:
            pass
    # scrollable이 더 길고 '원' 포함하면 전체 메뉴 → scrollable 우선
    if t_scroll and '원' in t_scroll and len(t_scroll) > len(t_best):
        return t_scroll
    if t_best and '원' in t_best:
        return t_best
    if t_scroll and '원' in t_scroll:
        return t_scroll
    # 재탐색: 전체 메뉴를 담은 요소(텍스트 가장 긴 것) 선택
    try:
        seen2 = set()
        again = []
        for cls in MENU_CLASS_CANDIDATES:
            for el in driver.find_elements(By.CLASS_NAME, cls):
                if id(el) not in seen2:
                    seen2.add(id(el))
                    again.append(el)
        if not again:
            again = driver.find_elements(By.CSS_SELECTOR, "[class*='menu'], [class*='Menu'], [class*='content']")
        if again:
            best2 = max(again, key=lambda el: (menu_score(get_menu_text(el)), len(get_menu_text(el))))
            t = get_menu_text(best2)
            if t:
                return t
            t = best2.text
            if t:
                return t
    except Exception:
        pass
    return t_best or t_scroll or best.text


def parse_menu_to_json(raw_text: str) -> dict:
    """크롤링한 메뉴 텍스트를 JSON으로 파싱. 가게 형식에 의존하지 않고 일반화된 휴리스틱 사용.

    - 가격: '숫자,숫자원' 형태 한 줄
    - 주문 수: '주문 N' 형태(있으면 수집)
    - 카테고리: 가격 직전까지 등장한 '짧은 줄'(길이 제한)을 섹션명으로 사용
    - 메뉴 한 개: 가격 줄 직전의 연속 줄 → 첫 줄(또는 첫 비태그 줄)을 이름, 나머지를 설명

    반환: { "store_name", "notice", "sections": [ { "name", "items": [ { "name", "description", "price", "price_value", "order_count", "category" } ] } ] }
    """
    if not raw_text or not raw_text.strip():
        return {"store_name": "", "notice": "", "sections": []}

    lines = [line.strip() for line in raw_text.splitlines() if line.strip()]
    skip_ui = {
        "이전 페이지", "닫기", "홈", "쿠폰", "소식", "메뉴", "예약", "리뷰", "사진", "정보",
        "포장", "배달", "검색", "추천", "품절 상품 제외", "맨위로", "마이플레이스",
        "설정된 언어", "한국어",
        # common navigation/overlay UI
        "페이지 닫기", "이미지 갯수", "알림받기", "출발", "도착", "저장", "거리뷰", "공유",
    }
    price_re = re.compile(r"^[\d,]+원$")
    order_re = re.compile(r"^주문 \d+$")
    # 메뉴명으로 쓰지 않을 짧은 태그(일부 가게에서만 사용)
    name_skip_tags = {"인기", "대표"}

    max_category_len = 45
    min_category_len = 2
    # 숫자 포함 또는 흔한 섹션어일 때만 카테고리로 사용 (짧은 메뉴명이 섹션으로 묶이지 않도록)
    section_like = {
        "인기", "대표", "추천", "식사", "사이드", "주류", "음료", "메뉴", "세트",
        "한식", "중식", "일식", "양식", "분식", "카페", "디저트", "안주", "사이드메뉴",
    }

    store_name = ""
    notice_lines = []
    all_items = []  # (category, item) 순서 유지
    current_category = ""
    buffer = []
    i = 0

    def is_ui_noise(s: str) -> bool:
        if not s:
            return True
        if s in skip_ui:
            return True
        # lightweight patterns for overlay/navigation labels
        if re.search(r"이미지\s*갯수", s):
            return True
        if re.search(r"(길찾기|거리뷰|공유|알림받기|페이지\s*닫기)", s):
            return True
        return False

    while i < len(lines):
        line = lines[i]
        # 가게명: 첫 번째로 나오는 의미 있는 짧은 줄(가격/주문/UI 제외)
        if not store_name and not is_ui_noise(line) and not price_re.match(line) and not order_re.match(line):
            if len(line) <= 80 and len(line) >= 2:
                store_name = line
        if "주문이 종료" in line or "주문 가능" in line:
            notice_lines.append(line)

        if price_re.match(line):
            name = ""
            desc_parts = []
            for b in buffer:
                if is_ui_noise(b) or (store_name and b == store_name):
                    continue
                if b in name_skip_tags:
                    continue
                if not name:
                    name = b
                else:
                    desc_parts.append(b)
            description = "\n".join(desc_parts) if desc_parts else ""
            if not name:
                # no valid name -> skip this item entirely
                i += 1
                if i < len(lines) and order_re.match(lines[i]):
                    i += 1
                buffer = []
                continue
            price_val = 0
            try:
                price_val = int(re.sub(r"[^\d]", "", line))
            except ValueError:
                pass
            item = {
                "name": name,
                "description": description,
                "price": line,
                "price_value": price_val,
                "order_count": None,
                "category": current_category or None,
            }
            i += 1
            if i < len(lines) and order_re.match(lines[i]):
                item["order_count"] = int(lines[i].replace("주문 ", ""))
                i += 1
            all_items.append((current_category, item))
            buffer = []
            continue

        if is_ui_noise(line):
            i += 1
            continue

        # 짧은 줄 + (숫자 포함 또는 섹션어)일 때만 카테고리로 사용
        is_short = min_category_len <= len(line) <= max_category_len
        looks_section = re.search(r"\d", line) or any(w in line for w in section_like)
        if is_short and looks_section and not price_re.match(line) and not order_re.match(line):
            current_category = line
            i += 1
            continue

        buffer.append(line)
        i += 1

    # 카테고리별로 묶되, 등장 순서 유지
    section_order = []
    section_items = {}
    for cat, item in all_items:
        key = cat or ""
        if key not in section_items:
            section_order.append(key)
            section_items[key] = []
        section_items[key].append(item)

    sections = [{"name": (name or "메뉴"), "items": section_items[name]} for name in section_order]

    return {
        "store_name": store_name,
        "notice": " ".join(notice_lines) if notice_lines else "",
        "sections": sections,
    }


def _safe_filename(name: str) -> str:
    """파일명에 쓸 수 없거나 비어 있는 문자 제거"""
    if not name or not name.strip():
        return "menu"
    s = name.strip()
    for c in r'\/:*?"<>|':
        s = s.replace(c, "")
    s = s.replace(" ", "_")
    return s or "menu"


def save_menu_json(data: dict, filepath: str = None) -> str:
    """메뉴 구조를 JSON 파일로 저장. filepath 미지정 시 {가게명}_menu.json 사용."""
    if filepath is None:
        store = data.get("store_name", "") or ""
        base = _safe_filename(store)
        filepath = f"{base}_menu.json"
    with open(filepath, "w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False, indent=2)
    return filepath


def build_menu_targets_from_places(places_path: str, targets_path: str = None) -> list:
    """ktb_res.json 형식에서 메뉴 크롤링 대상 목록을 생성."""
    with open(places_path, "r", encoding="utf-8") as f:
        data = json.load(f)
    places = data.get("places", []) if isinstance(data, dict) else []
    targets = []
    for pl in places:
        name = (pl.get("displayName") or {}).get("text")
        if not name:
            continue
        addr_short = pl.get("shortFormattedAddress")
        addr_full = pl.get("formattedAddress")
        query = name
        targets.append(
            {
                "id": pl.get("id"),
                "name": name,
                "primaryType": pl.get("primaryType"),
                "shortFormattedAddress": addr_short,
                "formattedAddress": addr_full,
                "googleMapsUri": pl.get("googleMapsUri"),
                "query": query,
            }
        )
    if targets_path:
        with open(targets_path, "w", encoding="utf-8") as f:
            json.dump(targets, f, ensure_ascii=False, indent=2)
    return targets


def load_menu_targets(targets_path: str) -> list:
    with open(targets_path, "r", encoding="utf-8") as f:
        data = json.load(f)
    if not isinstance(data, list):
        raise ValueError("menu_targets.json은 리스트 형식이어야 합니다.")
    return data


if __name__ == "__main__":
    import argparse
    import os

    parser = argparse.ArgumentParser(description="네이버 지도 메뉴 크롤러")
    parser.add_argument("--places", default="ktb_res.json", help="Google Places 결과 JSON")
    parser.add_argument("--targets", default="menu_targets.json", help="메뉴 크롤링 대상 JSON")
    parser.add_argument("--refresh-targets", action="store_true", help="places에서 targets 재생성")
    parser.add_argument("--start", type=int, default=0, help="시작 인덱스")
    parser.add_argument("--limit", type=int, default=0, help="최대 처리 개수(0이면 전체)")
    parser.add_argument("--out-dir", default="menus", help="메뉴 JSON 저장 폴더")
    parser.add_argument("--dry-run", action="store_true", help="검색어만 출력")
    args = parser.parse_args()

    if args.refresh_targets or not os.path.exists(args.targets):
        targets = build_menu_targets_from_places(args.places, args.targets)
        print(f"[targets 생성] {args.targets} ({len(targets)}개)")
    else:
        targets = load_menu_targets(args.targets)
        print(f"[targets 로드] {args.targets} ({len(targets)}개)")

    start = max(args.start, 0)
    end = None if args.limit == 0 else start + max(args.limit, 0)
    targets = targets[start:end]

    if args.dry_run:
        for t in targets:
            print(t.get("query") or t.get("name"))
        raise SystemExit(0)

    os.makedirs(args.out_dir, exist_ok=True)

    for idx, t in enumerate(targets, 1):
        query = t.get("query") or t.get("name")
        if not query:
            continue
        expected_name = _safe_filename(t.get("name") or query or "menu")
        filename = f"{expected_name}_menu.json"
        expected_path = os.path.join(args.out_dir, filename)
        uploaded_path = os.path.join("uploaded", filename)
        if os.path.exists(expected_path) or os.path.exists(uploaded_path):
            hit = expected_path if os.path.exists(expected_path) else uploaded_path
            print(f"[{idx}/{len(targets)}] 스킵(이미 있음): {hit}")
            continue
        print(f"[{idx}/{len(targets)}] 검색: {query}")
        try:
            result = menu(query)
        except (NoSuchWindowException, MaxRetryError, NewConnectionError, WebDriverException) as e:
            print(f"  - 드라이버 오류로 스킵: {type(e).__name__}")
            try:
                restart_driver()
            except Exception:
                pass
            continue
        except Exception as e:
            print(f"  - 예외로 스킵: {type(e).__name__}")
            continue
        if result == -1:
            print("  - 메뉴를 찾지 못함")
            continue
        data = parse_menu_to_json(result)
        if not data.get("store_name"):
            data["store_name"] = t.get("name", "")
        filename = f"{_safe_filename(data.get('store_name') or t.get('name','menu'))}_menu.json"
        out_path = os.path.join(args.out_dir, filename)
        save_menu_json(data, out_path)
        print(f"  - 저장: {out_path}")
