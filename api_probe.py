"""
CHZZK API 실증 탐색 스크립트

확인 항목:
  1. 채널 정보 응답 구조 및 필드
  2. VOD 목록 페이지네이션 방식 (cursor? page? offset?)
  3. 클립 목록 페이지네이션 방식
  4. sortType 옵션 실제 동작 확인
  5. 최대 size 파라미터 허용 범위
  6. 3개월치 데이터 도달까지 페이지 수 추정
  7. 클립-VOD 연결 필드(videoNo) 존재 여부
"""

import httpx
import asyncio
import json
import sys
from datetime import datetime, timedelta

if sys.platform == "win32":
    import io
    sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding="utf-8")

HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/123.0.0.0 Safari/537.36",
    "Referer": "https://chzzk.naver.com/",
    "Accept": "application/json, text/plain, */*",
}
BASE = "https://api.chzzk.naver.com/service/v1"

# 테스트 채널 (울프 — 활동량 많은 채널)
TEST_CHANNEL = "0b33823ac81de48d5b78a38cdbc0ab94"
THREE_MONTHS_AGO = datetime.now() - timedelta(days=90)


def section(title: str):
    print(f"\n{'='*60}")
    print(f"  {title}")
    print(f"{'='*60}")


def dump_keys(obj, indent=0):
    """dict/list 최상위 키와 값 타입만 출력 (응답 구조 파악용)"""
    prefix = "  " * indent
    if isinstance(obj, dict):
        for k, v in obj.items():
            if isinstance(v, (dict, list)):
                print(f"{prefix}{k}: {type(v).__name__}({len(v)})")
                if indent < 2:
                    dump_keys(v, indent + 1)
            else:
                print(f"{prefix}{k}: {repr(v)[:80]}")
    elif isinstance(obj, list) and obj:
        print(f"{prefix}[0]:")
        dump_keys(obj[0], indent + 1)


# ── 1. 채널 정보 ─────────────────────────────────────────────
async def probe_channel(client: httpx.AsyncClient):
    section("1. 채널 정보 응답 구조")
    res = await client.get(f"{BASE}/channels/{TEST_CHANNEL}", headers=HEADERS, timeout=10)
    data = res.json()
    print(f"HTTP {res.status_code}")
    dump_keys(data)
    content = data.get("content", {})
    print(f"\n  → channelId    : {content.get('channelId')}")
    print(f"  → channelName  : {content.get('channelName')}")
    print(f"  → followerCount: {content.get('followerCount')}")
    print(f"  → openLive     : {content.get('openLive')}")


# ── 2. VOD 페이지네이션 탐색 ────────────────────────────────
async def probe_videos_pagination(client: httpx.AsyncClient):
    section("2. VOD 목록 — 페이지네이션 구조 및 필드")
    url = f"{BASE}/channels/{TEST_CHANNEL}/videos"

    # 2-a. size=3 으로 응답 구조 먼저 확인
    res = await client.get(url, params={"size": 3}, headers=HEADERS, timeout=10)
    body = res.json()
    print(f"HTTP {res.status_code}  | size=3")
    content = body.get("content", {})
    dump_keys(content)

    print("\n  ── 첫 번째 VOD 항목 필드 ──")
    items = content.get("data", [])
    if items:
        dump_keys(items[0])

    # 2-b. 페이지네이션 키 탐색
    print("\n  ── 페이지네이션 관련 키 ──")
    for key in ["next", "cursor", "page", "totalCount", "hasNext", "lastVideoNo"]:
        if key in content:
            print(f"  → {key}: {content[key]}")
    # content 바깥에도 있을 수 있음
    for key in ["next", "cursor", "page", "totalCount", "hasNext"]:
        if key in body:
            print(f"  (root) → {key}: {body[key]}")

    # 2-c. size=100 시도
    print("\n  ── size=100 허용 여부 ──")
    res2 = await client.get(url, params={"size": 100}, headers=HEADERS, timeout=10)
    body2 = res2.json()
    items2 = body2.get("content", {}).get("data", [])
    print(f"  HTTP {res2.status_code} | 반환된 항목 수: {len(items2)}")

    # 2-d. 페이지 순회 — 3개월치 도달까지 몇 페이지?
    # 2-e. 최대 size 탐색
    section("2-e. 허용 size 파라미터 탐색")
    for sz in [20, 30, 40, 50]:
        r = await client.get(url, params={"size": sz, "page": 0}, headers=HEADERS, timeout=10)
        cnt = len(r.json().get("content", {}).get("data", []))
        print(f"  size={sz:3d} → HTTP {r.status_code}  반환 항목: {cnt}개")

    section("2-d. VOD 3개월치 페이지 순회")
    await paginate_videos(client, url, "videos")


# ── 3. 클립 페이지네이션 탐색 ───────────────────────────────
async def probe_clips_pagination(client: httpx.AsyncClient):
    section("3. 클립 목록 — 페이지네이션 구조 및 필드")
    url = f"{BASE}/channels/{TEST_CHANNEL}/clips"

    res = await client.get(url, params={"size": 3, "sortType": "LATEST"}, headers=HEADERS, timeout=10)
    body = res.json()
    print(f"HTTP {res.status_code}  | size=3, sortType=LATEST")
    content = body.get("content", {})
    dump_keys(content)

    print("\n  ── 첫 번째 클립 항목 필드 ──")
    items = content.get("data", [])
    if items:
        dump_keys(items[0])
        # 클립-VOD 연결 필드 확인
        print(f"\n  ── 클립-VOD 연결 필드 ──")
        for key in ["videoNo", "originVideoNo", "videoId", "vod"]:
            if key in items[0]:
                print(f"  → {key}: {items[0][key]}")
            else:
                print(f"  → {key}: (없음)")

    # 페이지네이션 키
    print("\n  ── 페이지네이션 관련 키 ──")
    for key in ["next", "cursor", "page", "totalCount", "hasNext", "lastVideoNo"]:
        if key in content:
            print(f"  → {key}: {content[key]}")

    # sortType=POPULAR 비교
    print("\n  ── sortType=POPULAR 반환 항목 수 ──")
    res_pop = await client.get(url, params={"size": 20, "sortType": "POPULAR"}, headers=HEADERS, timeout=10)
    items_pop = res_pop.json().get("content", {}).get("data", [])
    res_lat = await client.get(url, params={"size": 20, "sortType": "LATEST"}, headers=HEADERS, timeout=10)
    items_lat = res_lat.json().get("content", {}).get("data", [])
    print(f"  POPULAR: {len(items_pop)}개  LATEST: {len(items_lat)}개")
    if items_lat:
        first_date = items_lat[0].get("createdDate") or items_lat[0].get("publishDate")
        last_date = items_lat[-1].get("createdDate") or items_lat[-1].get("publishDate")
        print(f"  LATEST 첫 항목 날짜: {first_date}  마지막: {last_date}")

    # 커서 파라미터 형식 탐색
    await probe_clip_cursor_formats(client, url)

    # 3-b: clipUID 파라미터로 실제 순회
    section("3-b. 클립 3개월치 순회 (?clipUID={cursor} 방식)")
    await paginate_clips(client, url, "clips", cursor_param="clipUID", use_last_uid=False)


# ── 공통: 페이지 순회 함수 ──────────────────────────────────
async def paginate_videos(client, url, label):
    """VOD: page=0,1,2... 방식 순회. 3개월 이전 도달 또는 25페이지 초과 시 중단."""
    total = 0
    oldest_date = None

    for page_num in range(25):
        res = await client.get(url, params={"size": 20, "page": page_num}, headers=HEADERS, timeout=10)
        body = res.json()
        content = body.get("content", {})
        items = content.get("data", [])
        total_pages = content.get("totalPages", 0)

        if not items:
            print(f"  [{label}] 페이지 {page_num}: 항목 없음 → 종료")
            break

        total += len(items)
        raw_date = items[-1].get("publishDateAt") or items[-1].get("publishDate")
        oldest_date = _parse_date(raw_date)
        date_str = oldest_date.strftime("%Y-%m-%d") if oldest_date else "파싱불가"
        print(f"  [{label}] 페이지 {page_num:2d}/{total_pages-1}: {len(items)}건 | 마지막 날짜: {date_str}")

        if oldest_date and oldest_date < THREE_MONTHS_AGO:
            print(f"  → 3개월 이전 도달 — 종료")
            break
        if page_num + 1 >= total_pages:
            print(f"  → 마지막 페이지 도달 (전체 {total_pages}페이지)")
            break

    print(f"\n  [{label}] 총 {total}건 수집 | 가장 오래된 항목: {oldest_date.strftime('%Y-%m-%d') if oldest_date else '-'}")


async def probe_clip_cursor_formats(client, url):
    """클립 cursor 파라미터 형식 탐색 — 어떤 형식이 다음 페이지를 반환하는지 확인"""
    section("3-c. 클립 커서 파라미터 형식 탐색")

    # 1) 1페이지를 가져와서 커서 및 마지막 clipUID 확보
    res0 = await client.get(url, params={"size": 3, "sortType": "LATEST"}, headers=HEADERS, timeout=10)
    content0 = res0.json().get("content", {})
    items0 = content0.get("data", [])
    page_info = content0.get("page", {})
    cursor_uid = page_info.get("next", {}).get("clipUID") if page_info else None
    last_uid_in_page = items0[-1]["clipUID"] if items0 else None
    first_uid = items0[0]["clipUID"] if items0 else None

    print(f"  1페이지 clipUID 목록: {[i['clipUID'] for i in items0]}")
    print(f"  page.next.clipUID (커서): {cursor_uid}")
    print(f"  마지막 항목 clipUID     : {last_uid_in_page}")

    # 2) 다양한 파라미터 형식 시도
    candidates = [
        ("next",        cursor_uid,                                  "next={cursor_uid}"),
        ("next",        json.dumps({"clipUID": cursor_uid}),         "next=JSON({clipUID})"),
        ("clipUID",     cursor_uid,                                  "clipUID={cursor_uid}"),
        ("afterClipUID",cursor_uid,                                  "afterClipUID={cursor_uid}"),
        ("next",        last_uid_in_page,                            "next={last_item_clipUID}"),
        ("clipUID",     last_uid_in_page,                            "clipUID={last_item_clipUID}"),
    ]

    for param_key, param_val, label in candidates:
        if not param_val:
            continue
        params = {"size": 3, "sortType": "LATEST", param_key: param_val}
        r = await client.get(url, params=params, headers=HEADERS, timeout=10)
        body = r.json()
        content = body.get("content", {})
        items = content.get("data", [])
        uids = [i["clipUID"] for i in items]
        moved = uids[0] != first_uid if uids else False
        marker = "✅ 페이지 이동!" if moved else "❌ 동일 페이지"
        print(f"  {marker}  {label:<40} → {uids}")


async def paginate_clips(client, url, label, cursor_param="next", use_last_uid=False):
    """클립 cursor 방식 순회. cursor_param 으로 파라미터 이름 지정."""
    total = 0
    oldest_date = None
    cursor_val = None

    for page_num in range(25):
        params = {"size": 20, "sortType": "LATEST"}
        if cursor_val:
            params[cursor_param] = cursor_val

        res = await client.get(url, params=params, headers=HEADERS, timeout=10)
        body = res.json()
        content = body.get("content", {})
        items = content.get("data", [])

        if not items:
            print(f"  [{label}] 페이지 {page_num+1}: 항목 없음 → 종료")
            break

        total += len(items)
        raw_date = items[-1].get("createdDate")
        oldest_date = _parse_date(raw_date)
        date_str = oldest_date.strftime("%Y-%m-%d") if oldest_date else "파싱불가"

        # 다음 커서: page.next.clipUID 또는 마지막 항목 clipUID
        page_info = content.get("page", {})
        next_info = (page_info.get("next") or {}) if page_info else {}
        cursor_val = items[-1]["clipUID"] if use_last_uid else next_info.get("clipUID")

        print(f"  [{label}] 페이지 {page_num+1:2d}: {len(items)}건 | 마지막 날짜: {date_str} | cursor: {cursor_val}")

        if oldest_date and oldest_date < THREE_MONTHS_AGO:
            print(f"  → 3개월 이전 도달 — 종료")
            break
        if not cursor_val:
            print(f"  → cursor 없음 — 마지막 페이지")
            break

    print(f"\n  [{label}] 총 {total}건 | 가장 오래된 항목: {oldest_date.strftime('%Y-%m-%d') if oldest_date else '-'}")


def _parse_date(raw):
    if not raw:
        return None
    try:
        if isinstance(raw, (int, float)):
            if raw > 1e11:
                raw /= 1000
            return datetime.fromtimestamp(raw)
        return datetime.fromisoformat(str(raw).split("+")[0].rstrip("Z"))
    except Exception:
        return None


# ── 4. sortType 옵션 열거 ────────────────────────────────────
async def probe_sort_types(client: httpx.AsyncClient):
    section("4. 지원 sortType 확인 (VOD)")
    url = f"{BASE}/channels/{TEST_CHANNEL}/videos"
    for sort in ["LATEST", "POPULAR", "OLDEST", "VIEW_COUNT"]:
        try:
            res = await client.get(url, params={"size": 1, "sortType": sort}, headers=HEADERS, timeout=10)
            status = "✅" if res.status_code == 200 else "❌"
            count = len(res.json().get("content", {}).get("data", []))
            print(f"  {status} sortType={sort:<12} HTTP {res.status_code}  항목: {count}개")
        except Exception as e:
            print(f"  ❌ sortType={sort:<12} 오류: {e}")


# ── main ─────────────────────────────────────────────────────
async def main():
    print(f"탐색 대상 채널: {TEST_CHANNEL}")
    print(f"3개월 기준일  : {THREE_MONTHS_AGO.strftime('%Y-%m-%d')}")

    async with httpx.AsyncClient() as client:
        await probe_channel(client)
        await probe_videos_pagination(client)
        await probe_clips_pagination(client)
        await probe_sort_types(client)

    section("탐색 완료")
    print("위 결과를 바탕으로 init.sql 스키마 및 크롤러 페이지네이션 로직을 확정합니다.")


if __name__ == "__main__":
    asyncio.run(main())
