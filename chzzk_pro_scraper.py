import httpx
import asyncio
import json
import random
import sys
from datetime import datetime, timedelta

# Windows 인코딩 대응
if sys.platform == 'win32':
    import io
    sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding='utf-8')

class ChzzkProScraper:
    def __init__(self):
        self.headers = {
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/123.0.0.0 Safari/537.36",
            "Accept": "application/json, text/plain, */*",
            "Accept-Language": "ko-KR,ko;q=0.9,en-US;q=0.8,en;q=0.7",
            "Origin": "https://chzzk.naver.com",
            "Referer": "https://chzzk.naver.com/",
            "Sec-Ch-Ua": '"Google Chrome";v="123", "Not:A-Brand";v="8", "Chromium";v="123"',
            "Sec-Ch-Ua-Mobile": "?0",
            "Sec-Ch-Ua-Platform": '"Windows"',
            "Sec-Fetch-Dest": "empty",
            "Sec-Fetch-Mode": "cors",
            "Sec-Fetch-Site": "same-site",
        }
        self.semaphore = asyncio.Semaphore(3)

    def parse_date(self, date_val):
        """다양한 날짜 형식(문자열/타임스탬프) 대응"""
        if isinstance(date_val, str):
            try:
                return datetime.strptime(date_val, "%Y-%m-%d %H:%M:%S")
            except:
                return datetime.strptime(date_val.split('+')[0].strip(), "%Y-%m-%dT%H:%M:%S")
        elif isinstance(date_val, (int, float)):
            # 밀리초 단위 타임스탬프인 경우 처리
            if date_val > 1e11: date_val /= 1000
            return datetime.fromtimestamp(date_val)
        return datetime.now()

    async def fetch_with_retry(self, client, url, params=None):
        async with self.semaphore:
            await asyncio.sleep(random.uniform(0.5, 1.5))
            for attempt in range(3):
                try:
                    response = await client.get(url, params=params, timeout=10.0)
                    if response.status_code == 200:
                        return response.json()
                    await asyncio.sleep(2)
                except Exception as e:
                    await asyncio.sleep(2)
            return None

    async def get_recent_data(self, channel_id, days=2):
        now = datetime.now()
        threshold_date = now - timedelta(days=days)
        print(f"INFO: {threshold_date.strftime('%Y-%m-%d')} 이후의 데이터를 수집합니다.")

        async with httpx.AsyncClient(headers=self.headers, follow_redirects=True) as client:
            vod_url = f"https://api.chzzk.naver.com/service/v1/channels/{channel_id}/videos"
            clip_url = f"https://api.chzzk.naver.com/service/v1/channels/{channel_id}/clips"
            
            vod_res, clip_res = await asyncio.gather(
                self.fetch_with_retry(client, vod_url, {"size": 30, "sortType": "LATEST"}),
                self.fetch_with_retry(client, clip_url, {"size": 50, "sortType": "LATEST"})
            )

            results = {"vods": [], "clips": []}

            if vod_res and 'content' in vod_res:
                for v in vod_res['content'].get('data', []):
                    # 필드명 후보: publishDateAt, publishDate
                    date_key = 'publishDateAt' if 'publishDateAt' in v else 'publishDate'
                    pub_date = self.parse_date(v.get(date_key))
                    if pub_date >= threshold_date:
                        results['vods'].append({
                            "title": v['videoTitle'],
                            "category": v.get('videoCategoryValue', '미지정'),
                            "date": pub_date.strftime("%Y-%m-%d %H:%M:%S"),
                            "read_count": v['readCount'],
                            "link": f"https://chzzk.naver.com/video/{v['videoNo']}"
                        })

            if clip_res and 'content' in clip_res:
                for c in clip_res['content'].get('data', []):
                    create_date = self.parse_date(c.get('createdDate'))
                    if create_date >= threshold_date:
                        results['clips'].append({
                            "title": c['clipTitle'],
                            "date": create_date.strftime("%Y-%m-%d %H:%M:%S"),
                            "read_count": c['readCount'],
                            "link": f"https://chzzk.naver.com/clips/{c['clipUID']}"
                        })

            return results

async def main():
    WOLF_ID = "0b33823ac81de48d5b78a38cdbc0ab94"
    scraper = ChzzkProScraper()
    data = await scraper.get_recent_data(WOLF_ID, days=2)

    print(f"\n--- 수집 결과 요약 ---")
    print(f"채널 ID: {WOLF_ID}")
    print(f"최근 2일 VOD: {len(data['vods'])}건")
    print(f"최근 2일 클립: {len(data['clips'])}건")

    with open("wolf_recent_2days.json", "w", encoding="utf-8") as f:
        json.dump(data, f, indent=4, ensure_ascii=False)
    
    if data['vods']:
        print("\n[최신 VOD]")
        for v in data['vods'][:3]: print(f"- {v['title']} ({v['date']})")
    
    if data['clips']:
        print("\n[최신 클립]")
        for c in data['clips'][:3]: print(f"- {c['title']} ({c['date']})")

if __name__ == "__main__":
    asyncio.run(main())
