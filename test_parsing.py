import os
import requests
import xml.etree.ElementTree as ET
import pandas as pd
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

API_KEY = os.getenv("MOLIT_API_KEY")
API_URL = "https://apis.data.go.kr/1613000/RTMSDataSvcAptTradeDev/getRTMSDataSvcAptTradeDev"

def test_fetch_and_parse():
    # Test parameters: Gangnam-gu (11680), Recent month
    lawd_cd = "11680"
    deal_ymd = "202401"
    
    print(f">>> Fetching data for {lawd_cd} (Gangnam-gu) in {deal_ymd}...")
    full_url = f"{API_URL}?serviceKey={API_KEY}&LAWD_CD={lawd_cd}&DEAL_YMD={deal_ymd}&numOfRows=10&pageNo=1"
    
    try:
        response = requests.get(full_url, timeout=30)
        response.encoding = 'utf-8'
        if response.status_code != 200:
            print(f"Error: Status Code {response.status_code}")
            return

        xml_data = response.text
        print(">>> API Response received. Parsing...")
        
        # Parse Logic copied from apt_rank.py (with corrected tags)
        root = ET.fromstring(xml_data)
        items = root.findall(".//item")
        
        if not items:
            print("No items found in response.")
            return

        data = []
        for item in items:
            try:
                row = {
                    "아파트": (item.findtext("aptNm") or "").strip(),
                    "전용면적": float(item.findtext("excluUseAr")) if item.findtext("excluUseAr") else 0.0,
                    "층": item.findtext("floor") or "0",
                }
                data.append(row)
            except Exception as e:
                print(f"Parsing error: {e}")
                continue
        
        df = pd.DataFrame(data)
        
        print("\n>>> Parsed Data Sample (Top 5):")
        print(df.head())
        
        if '전용면적' in df.columns and '층' in df.columns:
            print("\n>>> SUCCESS: '전용면적' and '층' columns are present.")
            if df['전용면적'].sum() > 0:
                 print(">>> SUCCESS: '전용면적' has valid non-zero values.")
            else:
                 print(">>> WARNING: '전용면적' values are all zero (Tag might be wrong).")
                 
            if df['층'].nunique() > 1:
                print(">>> SUCCESS: '층' has valid values.")
        else:
            print("\n>>> FAIL: Columns are missing.")
            
    except Exception as e:
        print(f"Error during test: {e}")

if __name__ == "__main__":
    test_fetch_and_parse()
