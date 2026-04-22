import httpx
import asyncio
import json
import sys
from datetime import datetime
from typing import List, Dict, Optional

# Windows 한글 출력 대응
if sys.platform == 'win32':
    import io
    sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding='utf-8')

class ChzzkBase:
    """공통 설정 및 헤더 관리"""
    def __init__(self):
        self.headers = {
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/123.0.0.0 Safari/537.36",
            "Referer": "https://chzzk.naver.com/",
            "Accept": "application/json, text/plain, */*",
        }
        self.base_url = "https://api.chzzk.naver.com/service/v1"
        self.timeout = 30.0 # 타임아웃 30초로 증대

class StreamerManager(ChzzkBase):
    """[1] 스트리머/채널 정보 관리"""
    async def get_info(self, client: httpx.AsyncClient, channel_id: str) -> Dict:
        url = f"{self.base_url}/channels/{channel_id}"
        res = await client.get(url, headers=self.headers, timeout=self.timeout)
        data = res.json().get('content', {})
        return {
            "id": data.get('channelId'),
            "name": data.get('channelName'),
            "image": data.get('channelImageUrl'),
            "followers": data.get('followerCount'),
            "is_live": data.get('openLive', False),
            "raw": data
        }

class VODManager(ChzzkBase):
    """[2] 다시보기(VOD) 정보 관리 및 태그 소스 제공"""
    async def get_list(self, client: httpx.AsyncClient, channel_id: str, size: int = 10) -> List[Dict]:
        url = f"{self.base_url}/channels/{channel_id}/videos"
        res = await client.get(url, params={"size": size}, headers=self.headers, timeout=self.timeout)
        videos = res.json().get('content', {}).get('data', [])
        
        processed = []
        for v in videos:
            processed.append({
                "video_no": v['videoNo'],
                "title": v['videoTitle'],
                "category": v.get('videoCategoryValue', '미지정'),
                "tags": v.get('tags', []),
                "published_at": v['publishDateAt'],
                "raw": v
            })
        return processed

class ClipManager(ChzzkBase):
    """[3] 클립 정보 관리 및 지능형 태깅"""
    async def get_list(self, client: httpx.AsyncClient, channel_id: str, size: int = 10) -> List[Dict]:
        url = f"{self.base_url}/channels/{channel_id}/clips"
        res = await client.get(url, params={"size": size, "sortType": "POPULAR"}, headers=self.headers, timeout=self.timeout)
        clips = res.json().get('content', {}).get('data', [])
        return clips

    def enrich_tags(self, clip: Dict, vod_map: Dict[int, Dict]) -> List[str]:
        """클립에 빈약한 태그를 VOD 정보를 기반으로 보강"""
        tags = []
        video_no = clip.get('videoNo')
        
        # 1. 원본 VOD가 있다면 해당 VOD의 카테고리와 태그 상속
        if video_no and video_no in vod_map:
            vod = vod_map[video_no]
            if vod['category'] != '미지정':
                tags.append(vod['category'])
            if vod['tags']:
                tags.extend(vod['tags'])
        
        # 2. 클립 제목 기반 키워드 추출 (간이 NLP)
        title = clip['clipTitle']
        keywords = ["레전드", "웃긴", "박제", "T1", "울프", "LCK", "반응"]
        for kw in keywords:
            if kw.lower() in title.lower():
                tags.append(kw)
        
        return list(sorted(set(tags))) # 중복 제거 및 정렬

async def main():
    channel_id = "0b33823ac81de48d5b78a38cdbc0ab94" # 울프
    
    async with httpx.AsyncClient() as client:
        s_mgr = StreamerManager()
        v_mgr = VODManager()
        c_mgr = ClipManager()

        print(f"INFO: '{channel_id}' 데이터 수집 및 지능형 태깅 시작...")
        
        try:
            # 1. 스트리머 & VOD 정보 수집
            streamer_info, vod_list = await asyncio.gather(
                s_mgr.get_info(client, channel_id),
                v_mgr.get_list(client, channel_id)
            )
            
            vod_map = {v['video_no']: v for v in vod_list}

            # 2. 클립 수집
            raw_clips = await c_mgr.get_list(client, channel_id)

            # 3. 지능형 태깅 적용
            final_clips = []
            for c in raw_clips:
                enriched_tags = c_mgr.enrich_tags(c, vod_map)
                final_clips.append({
                    "clip_id": c['clipUID'],
                    "title": c['clipTitle'],
                    "tags": enriched_tags,
                    "origin_vod": c.get('videoNo'),
                    "read_count": c['readCount'],
                    "created_at": c['createdDate']
                })

            # 결과 저장
            output = {
                "metadata": {
                    "collected_at": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
                    "channel_id": channel_id
                },
                "streamer": streamer_info,
                "vods": vod_list,
                "clips": final_clips
            }
            
            with open("chzzk_modular_data.json", "w", encoding="utf-8") as f:
                json.dump(output, f, indent=4, ensure_ascii=False)
                
            print(f"SUCCESS: '{streamer_info['name']}' 데이터 수집 완료!")
            if final_clips:
                print(f"태깅 예시: {final_clips[0]['title']}")
                print(f"ㄴ 생성된 태그: {final_clips[0]['tags']}")

        except Exception as e:
            print(f"ERROR: 수집 도중 오류 발생: {e}")

if __name__ == "__main__":
    asyncio.run(main())
