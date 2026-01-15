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
import threading
import traceback
from concurrent.futures import ThreadPoolExecutor, as_completed
from dateutil.relativedelta import relativedelta

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

# Global Status for Background Task
UPDATE_STATUS = {'running': False, 'message': ''}

def fetch_data(lawd_cd, deal_ymd):
    if not API_KEY: 
        print(f"Error: API Key is missing", flush=True)
        return None
    full_url = f"{API_URL}?serviceKey={API_KEY}&LAWD_CD={lawd_cd}&DEAL_YMD={deal_ymd}&numOfRows=9999&pageNo=1"
    try:
        response = requests.get(full_url, timeout=30)
        response.encoding = 'utf-8'
        if response.status_code != 200:
            print(f"[API Error] Status: {response.status_code} for {lawd_cd}/{deal_ymd}", flush=True)
            return None
        return response.text
    except Exception as e:
        print(f"[API Exception] {lawd_cd}/{deal_ymd}: {e}", flush=True)
        return None

def parse_xml_to_df(xml_data, district_name):
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

def get_area_tier(area):
    if area < 50: return 10
    elif area < 70: return 20
    elif area < 102: return 30
    elif area < 135: return 40
    else: return 50

def analyze_data(df):
    if df.empty: return pd.DataFrame()
    grouped = df.groupby(["자치구", "법정동", "아파트", "년", "전용면적"]).agg(
        거래건수=("거래금액", "count"),
        평균거래금액=("거래금액", "mean"),
        최근거래일=("거래일자", "max")
    ).reset_index()
    grouped = grouped.sort_values(by="거래건수", ascending=False)
    grouped["가격대"] = grouped["평균거래금액"].apply(get_price_tier)
    grouped["평형대"] = grouped["전용면적"].apply(get_area_tier)
    return grouped

def collect_and_save_data():
    print(">>> Starting Data Collection...", flush=True)
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
            
    print(f">>> Total Tasks to fetch: {len(tasks)}", flush=True)
    all_dfs = []
    completed = 0
    with ThreadPoolExecutor(max_workers=12) as executor:
        future_to_task = {executor.submit(fetch_data, c, y): (c, n, y) for c, n, y in tasks}
        for future in as_completed(future_to_task):
            completed += 1
            if completed % 100 == 0:
                print(f">>> Progress: {completed}/{len(tasks)} ({completed/len(tasks)*100:.1f}%)", flush=True)
            
            _, name, _ = future_to_task[future]
            try:
                xml_txt = future.result()
                if xml_txt:
                    df = parse_xml_to_df(xml_txt, name)
                    if not df.empty: all_dfs.append(df)
            except Exception as e: 
                print(f"[Task Error] {name}: {e}", flush=True)

    print(f">>> Fetching Finished. Blocks found: {len(all_dfs)}", flush=True)
    if not all_dfs: return False, "데이터 수집 실패: API 응답이 없거나 네트워크 오류입니다."
    try:
        full_df = pd.concat(all_dfs, ignore_index=True)
        print(f">>> Saving raw data ({len(full_df)} rows)...", flush=True)
        full_df.to_csv(RAW_CSV_FILENAME, index=False, encoding='utf-8-sig')
        
        print(">>> Analyzing & Saving ranking data...", flush=True)
        analyzed_df = analyze_data(full_df)
        analyzed_df.to_csv(CSV_FILENAME, index=False, encoding='utf-8-sig')
        print(">>> All Data Processes Completed Successfully!", flush=True)
        return True, f"수집 완료: 총 {len(full_df)}건의 거래 데이터 분석됨."
    except Exception as e: 
        print(f"[Save Error] {e}", flush=True)
        return False, f"데이터 처리 중 오류: {str(e)}"

def validate_data_file():
    if not os.path.exists(CSV_FILENAME): return False
    try:
        df = pd.read_csv(CSV_FILENAME)
        return not df.empty and '년' in df.columns
    except: return False

@app.route('/')
def index():
    if not validate_data_file():
        return '''<div style="text-align:center; padding:50px; font-family:sans-serif;">
            <h1>데이터 파일이 없습니다.</h1>
            <p>서버에서 수집을 시작해주세요. (약 3~5분 소요)</p>
            <button onclick="this.disabled=true; this.innerText='수집 시작됨...'; fetch('/update', {method:'POST'})" 
                    style="padding:15px 30px; cursor:pointer; background:#4F46E5; color:white; border:none; border-radius:8px;">
                🔄 데이터 수집 시작
            </button>
        </div>'''
    
    try:
        mtime = os.path.getmtime(CSV_FILENAME)
        last_updated = datetime.datetime.fromtimestamp(mtime).strftime('%Y-%m-%d %H:%M')
        df = pd.read_csv(CSV_FILENAME).fillna(0)
        records = df.to_dict('records')
        years_list = sorted(df['년'].unique().tolist())
        district_list = sorted(list(SEOUL_DISTRICTS.values()))
        return render_template('index.html', data_json=json.dumps(records), years=years_list, districts=district_list, last_updated=last_updated)
    except Exception as e: return f"Error: {str(e)}"

@app.route('/update', methods=['POST'])
def update_data():
    global UPDATE_STATUS
    if UPDATE_STATUS['running']:
        return jsonify({'status': 'error', 'message': '이미 데이터 수집이 진행 중입니다. 잠시 후 다시 시도해주세요.'})

    def task():
        global UPDATE_STATUS
        try:
            UPDATE_STATUS['running'] = True
            UPDATE_STATUS['message'] = '데이터 수집 중...'
            print(">>> Background Update Started", flush=True)
            
            success, msg = collect_and_save_data()
            
            UPDATE_STATUS['running'] = False
            UPDATE_STATUS['message'] = 'success' if success else f'error: {msg}'
            print(f">>> Background Update Finished: {msg}", flush=True)
        except Exception as e:
            traceback.print_exc()
            UPDATE_STATUS['running'] = False
            UPDATE_STATUS['message'] = f'error: {str(e)}'

    thread = threading.Thread(target=task)
    thread.daemon = True 
    thread.start()
    return jsonify({'status': 'success', 'message': '수집 작업이 시작되었습니다. 완료 시 알림이 뜹니다.'})

@app.route('/update/status', methods=['GET'])
def get_update_status():
    return jsonify(UPDATE_STATUS)

@app.route('/api/data', methods=['GET'])
def get_data_api():
    if not validate_data_file(): return jsonify({'status': 'error'}), 404
    df = pd.read_csv(CSV_FILENAME).fillna(0)
    return jsonify({'status': 'success', 'data': df.to_dict('records')})

@app.route('/api/history', methods=['GET'])
def get_apt_history():
    apt_name = request.args.get('apt_name')
    dong = request.args.get('dong')
    if not os.path.exists(RAW_CSV_FILENAME): return jsonify({'status': 'error'}), 404
    df = pd.read_csv(RAW_CSV_FILENAME)
    mask = (df['아파트'] == apt_name) & (df['법정동'] == dong)
    filtered = df[mask].fillna(0).sort_values(by='거래일자', ascending=False)
    return jsonify({'status': 'success', 'data': filtered.to_dict('records')})

if __name__ == "__main__":
    app.run(host='0.0.0.0', port=5000)