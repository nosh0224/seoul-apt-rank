import os
import requests
from dotenv import load_dotenv

load_dotenv()

API_KEY = os.getenv("MOLIT_API_KEY")
API_URL = "https://apis.data.go.kr/1613000/RTMSDataSvcAptTradeDev/getRTMSDataSvcAptTradeDev"

def test_api():
    # Test with Gangnam-gu (11680), Jan 2024
    lawd_cd = "11680"
    deal_ymd = "202401"
    
    # 1. Try with Key AS-IS (from .env)
    print(f"--- Testing API Key: {API_KEY[:10]}... ---")
    url = f"{API_URL}?serviceKey={API_KEY}&LAWD_CD={lawd_cd}&DEAL_YMD={deal_ymd}&numOfRows=1&pageNo=1"
    
    try:
        res = requests.get(url, timeout=10)
        print(f"Status Code: {res.status_code}")
        print("Response Content (First 1000 chars):")
        print(res.text[:1000])
    except Exception as e:
        print(f"Error: {e}")

if __name__ == "__main__":
    test_api()
