import os
import sys
import argparse
import requests
import pandas as pd
import datetime
import json
import webbrowser
import time
import xml.etree.ElementTree as ET
from dotenv import load_dotenv
from tqdm import tqdm
from flask import Flask, render_template, jsonify, request
from threading import Timer
from concurrent.futures import ThreadPoolExecutor, as_completed
from dateutil.relativedelta import relativedelta

# ... imports remain the same ...

# Load environment variables
load_dotenv()

# Configuration
API_KEY = os.getenv("MOLIT_API_KEY")
API_URL = "https://apis.data.go.kr/1613000/RTMSDataSvcAptTradeDev/getRTMSDataSvcAptTradeDev"
CSV_FILENAME = "seoul_apt_ranking.csv"
RAW_CSV_FILENAME = "seoul_apt_trades.csv"

# Seoul District Codes
SEOUL_DISTRICTS = {
    "11110": "종로구", "11140": "중구", "11170": "용산구", "11200": "성동구",
    "11215": "광진구", "11230": "동대문구", "11260": "중랑구", "11290": "성북구",
    "11305": "강북구", "11320": "도봉구", "11350": "노원구", "11380": "은평구",
    "11410": "서대문구", "11440": "마포구", "11470": "양천구", "11500": "강서구",
    "11530": "구로구", "11545": "금천구", "11560": "영등포구", "11590": "동작구",
    "11620": "관악구", "11650": "서초구", "11680": "강남구", "11710": "송파구",
    "11740": "강동구"
}

# Initialize Flask App (Global Scope for Gunicorn)
app = Flask(__name__, template_folder='templates')

def fetch_data(lawd_cd, deal_ymd):
    # ... implementation remains the same ...
    if not API_KEY: return None
    full_url = f"{API_URL}?serviceKey={API_KEY}&LAWD_CD={lawd_cd}&DEAL_YMD={deal_ymd}&numOfRows=9999&pageNo=1"
    try:
        response = requests.get(full_url, timeout=30)
        response.encoding = 'utf-8'
        if response.status_code != 200: return None
        return response.text
    except Exception: return None

def parse_xml_to_df(xml_data, district_name):
    # ... implementation remains the same ...
    if not xml_data: return pd.DataFrame()
    try:
        root = ET.fromstring(xml_data)
        items = root.findall(".//item")
        if not items: return pd.DataFrame()
        data = []
        for item in items:
            try:
                row = {
                    "자치구": district_name,
                    "법정동": (item.findtext("umdNm") or item.findtext("aptDong") or "").strip(),
                    "아파트": (item.findtext("aptNm") or "").strip(),
                    "거래금액": int(item.findtext("dealAmount").strip().replace(",", "")) if item.findtext("dealAmount") else 0,
                    "년": item.findtext("dealYear"),
                    "월": item.findtext("dealMonth"),
                    "일": item.findtext("dealDay"),
                    "전용면적": float(item.findtext("excluUseAr")) if item.findtext("excluUseAr") else 0.0,
                    "층": item.findtext("floor") or "0",
                }
                if row["년"] and row["월"] and row["일"]:
                    row["거래일자"] = f"{row['년']}-{row['월'].zfill(2)}-{row['일'].zfill(2)}"
                else: row["거래일자"] = "2000-01-01"
                data.append(row)
            except: continue
        return pd.DataFrame(data)
    except: return pd.DataFrame()

def get_price_tier(price):
    if price < 100000: return "10억 미만"
    elif price < 150000: return "10억~15억"
    elif price < 200000: return "15억~20억"
    else: return "20억 이상"

def analyze_data(df):
    if df.empty: return pd.DataFrame()
    # Group by District, Dong, Apt, Year, AND Area
    grouped = df.groupby(["자치구", "법정동", "아파트", "년", "전용면적"]).agg(
        거래건수=("거래금액", "count"),
        평균거래금액=("거래금액", "mean"),
        최근거래일=("거래일자", "max")
    ).reset_index()
    grouped = grouped.sort_values(by="거래건수", ascending=False)
    grouped["가격대"] = grouped["평균거래금액"].apply(get_price_tier)
    return grouped

def collect_and_save_data():
    print(">>> Starting Data Collection...")
    end_date = datetime.date.today()
    start_date = end_date - relativedelta(years=3)
    months = []
    curr = start_date
    while curr <= end_date:
        months.append(curr.strftime("%Y%m"))
        curr += relativedelta(months=1)
    
    tasks = []
    for district_code, district_name in SEOUL_DISTRICTS.items():
        for m in months: tasks.append((district_code, district_name, m))
            
    all_dfs = []
    with ThreadPoolExecutor(max_workers=12) as executor:
        future_to_task = {executor.submit(fetch_data, c, y): (c, n, y) for c, n, y in tasks}
        for future in as_completed(future_to_task):
            _, name, _ = future_to_task[future]
            try:
                xml_txt = future.result()
                if xml_txt:
                    df = parse_xml_to_df(xml_txt, name)
                    if not df.empty: all_dfs.append(df)
            except: pass

    if not all_dfs: return False, "데이터 수집 실패: API 응답이 없거나 네트워크 오류입니다."
    try:
        full_df = pd.concat(all_dfs, ignore_index=True)
        # Save raw data for detail view
        full_df.to_csv(RAW_CSV_FILENAME, index=False, encoding='utf-8-sig')
        
        analyzed_df = analyze_data(full_df)
        analyzed_df.to_csv(CSV_FILENAME, index=False, encoding='utf-8-sig')
        return True, f"수집 완료: 총 {len(full_df)}건의 거래 데이터 분석됨."
    except Exception as e: return False, f"데이터 처리 중 오류: {str(e)}"

def validate_data_file():
    """Checks if the CSV file exists and has the required columns."""
    if not os.path.exists(CSV_FILENAME):
        return False
    try:
        df = pd.read_csv(CSV_FILENAME)
        if df.empty or '년' not in df.columns or '자치구' not in df.columns:
            return False
        return True
    except:
        return False

# Routes
@app.route('/')
def index():
    # Validate file before loading
    if not validate_data_file():
        return '''<div style="text-align:center; padding:50px; font-family:sans-serif;">
            <h1>데이터 파일이 없습니다.</h1>
            <p>로컬에서 수집된 데이터(seoul_apt_ranking.csv)를 함께 배포해주세요.</p>
            <p>지금 바로 서버에서 수집을 시작할 수도 있습니다. (1~2분 소요)</p>
            <button onclick="this.disabled=true; this.innerText='데이터 수집 중...'; fetch('/update', {method:'POST'}).then(r=>location.reload())" 
                    style="padding:15px 30px; cursor:pointer; background:#4F46E5; color:white; border:none; border-radius:8px; font-size:16px; font-weight:bold;">
                🔄 데이터 수집 및 복구 시작
            </button>
        </div>'''
    
    try:
        # Get file modification time
        mtime = os.path.getmtime(CSV_FILENAME)
        last_updated = datetime.datetime.fromtimestamp(mtime).strftime('%Y-%m-%d %H:%M')
        
        df = pd.read_csv(CSV_FILENAME)
        df = df.fillna(0)
        records = df.to_dict('records')
        years_list = sorted(df['년'].unique().tolist())
        district_list = sorted(list(SEOUL_DISTRICTS.values()))
        
        return render_template('index.html', 
                             data_json=json.dumps(records), 
                             years=years_list, 
                             districts=district_list,
                             last_updated=last_updated)
    except Exception as e:
        return f"Error: {str(e)}"
        mod_time = os.path.getmtime(CSV_FILENAME)
        last_updated = datetime.datetime.fromtimestamp(mod_time).strftime('%Y-%m-%d %H:%M:%S')
        
        return render_template('index.html', 
                                districts=district_list, 
                                years=years_list, 
                                data_json=json.dumps(records, ensure_ascii=False),
                                last_updated=last_updated)
    except Exception as e:
        return f'''<div style="padding:20px;">
                    <h3>처리 중 오류가 발생했습니다.</h3>
                    <pre>{str(e)}</pre>
                    <p>파일 형식이 올바르지 않은 것 같습니다. 데이터를 초기화하시겠습니까?</p>
                    <button onclick="fetch('/update', {{method:'POST'}}).then(r=>location.reload())">데이터 재수집</button>
                    </div>'''


import threading

# ... (기존 코드)

@app.route('/update', methods=['POST'])
def update_data():
    """백그라운드 스레드에서 데이터 수집을 시작하고 즉시 응답 반환"""
    def task():
        try:
            print(">>> Background Update Started")
            collect_and_save_data()
            print(">>> Background Update Finished")
        except Exception as e:
            print(f">>> Background Update Error: {e}")

    # 백그라운드 스레드 실행
    thread = threading.Thread(target=task)
    thread.daemon = True  # 메인 프로세스 종료 시 함께 종료
    thread.start()

    return jsonify({
        'status': 'success', 
        'message': '데이터 수집 요청이 서버에 전달되었습니다.\n작업은 백그라운드에서 진행되며 약 3~5분 소요됩니다.\n잠시 후 새로고침하여 확인해주세요.'
    })

@app.route('/api/data', methods=['GET'])
def get_data_api():
    """External API endpoint to fetch the raw data as JSON."""
    if not validate_data_file():
        return jsonify({'status': 'error', 'message': 'Data file not found'}), 404
    try:
        df = pd.read_csv(CSV_FILENAME)
        df = df.fillna(0)
        records = df.to_dict('records')
        return jsonify({'status': 'success', 'count': len(records), 'data': records})
    except Exception as e:
        return jsonify({'status': 'error', 'message': str(e)}), 500

@app.route('/api/history', methods=['GET'])
def get_apt_history():
    """Returns detailed trade history for a specific apartment."""
    apt_name = request.args.get('apt_name')
    dong = request.args.get('dong')
    
    if not os.path.exists(RAW_CSV_FILENAME):
            return jsonify({'status': 'error', 'message': '상세 데이터 파일이 없습니다. 데이터를 업데이트해주세요.'}), 404
            
    try:
        df = pd.read_csv(RAW_CSV_FILENAME)
        # Filter by Apt Name and Dong
        mask = (df['아파트'] == apt_name) & (df['법정동'] == dong)
        filtered = df[mask].fillna(0)
        
        # Sort by Date desc
        if '거래일자' in filtered.columns:
            filtered = filtered.sort_values(by='거래일자', ascending=False)
        
        records = filtered.to_dict('records')
        return jsonify({'status': 'success', 'data': records})
    except Exception as e:
        return jsonify({'status': 'error', 'message': str(e)}), 500

if __name__ == "__main__":
    # For local development
    app.run(host='0.0.0.0', port=5000)
