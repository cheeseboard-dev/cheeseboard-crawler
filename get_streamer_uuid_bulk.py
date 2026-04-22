import os
import csv
import json
import time
import urllib.parse
from datetime import datetime
from selenium import webdriver
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from webdriver_manager.chrome import ChromeDriverManager

def get_real_streamer_info(driver, keyword):
    """
    스트리머 명을 검색하여 channelId(UUID) 및 정보를 가져옵니다.
    """
    try:
        encoded_keyword = urllib.parse.quote(keyword)
        url = f"https://chzzk.naver.com/search?query={encoded_keyword}&type=channel"
        driver.get(url)
        
        # 페이지 로딩 대기
        time.sleep(3)
        
        # 사용자님이 알려주신 핵심 클래스를 가진 a 태그 찾기
        links = driver.find_elements(By.CSS_SELECTOR, "a[class*='channel_item_thumbnail']")
        
        if links:
            # 첫 번째 결과가 가장 유력하므로 첫 번째 채널 정보 추출
            link = links[0]
            href = link.get_attribute("href")
            channel_id = href.rstrip('/').split('/')[-1]
            
            # channel_id가 32자리 UUID인지 최종 확인
            if len(channel_id) != 32:
                return None
                
            # 이미지 및 이름 추출 시도
            try:
                img_el = link.find_element(By.TAG_NAME, "img")
                profile_image = img_el.get_attribute("src")
                channel_name = img_el.get_attribute("alt") or keyword
            except:
                profile_image = ""
                channel_name = keyword

            return {
                "channel_id": channel_id,
                "channel_name": channel_name,
                "profile_image": profile_image,
                "updated_at": datetime.now().strftime("%Y-%m-%d %H:%M:%S")
            }
    except Exception as e:
        print(f"ERROR ({keyword}): {e}")
        
    return None

def main():
    # 1. 대상 이름 읽기
    if not os.path.exists("target_names.txt"):
        print("target_names.txt 파일이 없습니다. parse_names.py를 먼저 실행하세요.")
        return
        
    with open("target_names.txt", "r", encoding="utf-8") as f:
        names = [line.strip() for line in f.readlines() if line.strip()]

    # 2. 브라우저 설정 (Headless)
    chrome_options = Options()
    chrome_options.add_argument("--headless")
    chrome_options.add_argument("user-agent=Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36")

    service = Service(ChromeDriverManager().install())
    driver = webdriver.Chrome(service=service, options=chrome_options)
    
    # 3. CSV 준비
    filename = "streamers.csv"
    fieldnames = ["channel_id", "channel_name", "profile_image", "updated_at"]
    
    # 이미 존재하는 ID는 건너뛰기 위해 기존 데이터 읽기
    existing_ids = set()
    if os.path.exists(filename):
        with open(filename, mode='r', encoding='utf-8-sig') as f:
            reader = csv.DictReader(f)
            for row in reader:
                existing_ids.add(row["channel_id"])
    
    # 4. 순차 검색 및 저장
    try:
        for name in names:
            print(f"처리 중: {name}...")
            info = get_real_streamer_info(driver, name)
            
            if info and info["channel_id"] not in existing_ids:
                file_exists = os.path.isfile(filename)
                with open(filename, mode='a', newline='', encoding='utf-8-sig') as f:
                    writer = csv.DictWriter(f, fieldnames=fieldnames)
                    if not file_exists:
                        writer.writeheader()
                    writer.writerow(info)
                    existing_ids.add(info["channel_id"])
                print(f"SUCCESS: {info['channel_name']} ({info['channel_id']}) 저장.")
            
            # 검색 간격 (서버 부하 방지 및 차단 회피)
            time.sleep(1)
            
    finally:
        driver.quit()

if __name__ == "__main__":
    main()
