from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import requests
import os
import psycopg2

# ── 기본 설정 ─────────────────────────────────────────────────────────
default_args = {
    'owner': 'airflow',
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

API_KEY = os.environ.get('OPENWEATHER_API_KEY')  # .env에서 가져옴
DB_CONN = "postgresql://airflow:airflow@postgres/airflow"
BASE_URL = "https://apis.data.go.kr/1360000/VilageFcstInfoService_2.0/getUltraSrtNcst"

# 도시별 격자 좌표
CITIES = [
    {'name': 'Seoul', 'nx': 60, 'ny': 127},
    {'name': 'Busan', 'nx': 98, 'ny': 76},
    {'name': 'Jeju',  'nx': 52, 'ny': 38},
]


# ── Task 1: Extract ───────────────────────────────────────────────────
# 기상청 API 호출 → raw 응답을 XCom에 저장
def extract(**context):
    # API는 정시 기준이라 현재 시각에서 1시간 전을 base_time으로 사용
    now = datetime.now() - timedelta(hours=1)
    base_date = now.strftime('%Y%m%d')
    base_time = now.strftime('%H00')

    results = []
    for city in CITIES:
        params = {
            'serviceKey': API_KEY,
            'numOfRows': 10,
            'pageNo': 1,
            'dataType': 'JSON',
            'base_date': base_date,
            'base_time': base_time,
            'nx': city['nx'],
            'ny': city['ny'],
        }
        response = requests.get(BASE_URL, params=params)
        response.raise_for_status()
        data = response.json()

        # API 오류 체크
        result_code = data['response']['header']['resultCode']
        if result_code != '00':
            raise ValueError(f"{city['name']} API 오류: {data['response']['header']['resultMsg']}")

        results.append({
            'city': city['name'],
            'items': data['response']['body']['items']['item'],
            'base_date': base_date,
            'base_time': base_time,
        })

    context['ti'].xcom_push(key='raw_data', value=results)
    print(f"✅ {len(results)}개 도시 데이터 수집 완료 ({base_date} {base_time})")


# ── Task 2: Transform ─────────────────────────────────────────────────
# category 배열 → 필요한 필드만 추출해서 딕셔너리로 변환
def transform(**context):
    raw_data = context['ti'].xcom_pull(key='raw_data')

    transformed = []
    for entry in raw_data:
        # category 배열을 딕셔너리로 변환
        # [{'category': 'T1H', 'obsrValue': '13.1'}, ...] → {'T1H': '13.1', ...}
        cat = {item['category']: item['obsrValue'] for item in entry['items']}

        transformed.append({
            'city':       entry['city'],
            'base_date':  entry['base_date'],
            'base_time':  entry['base_time'],
            'temp':       float(cat.get('T1H', 0)),   # 기온 (℃)
            'humidity':   int(cat.get('REH', 0)),      # 습도 (%)
            'wind_speed': float(cat.get('WSD', 0)),    # 풍속 (m/s)
            'rain_type':  int(cat.get('PTY', 0)),      # 강수형태 (0=없음, 1=비, 3=눈)
            'rain_1h':    float(cat.get('RN1', 0)),    # 1시간 강수량 (mm)
        })

    context['ti'].xcom_push(key='transformed_data', value=transformed)
    print(f"✅ 변환 완료: {transformed}")


# ── Task 3: Load ──────────────────────────────────────────────────────
# 변환된 데이터를 PostgreSQL에 저장
def load(**context):
    data = context['ti'].xcom_pull(key='transformed_data')

    conn = psycopg2.connect(DB_CONN)
    cur = conn.cursor()

    for row in data:
        cur.execute("""
            INSERT INTO weather_raw
                (city, temp_celsius, humidity, wind_speed, rain_type, rain_1h, collected_at)
            VALUES (%s, %s, %s, %s, %s, %s, NOW())
        """, (row['city'], row['temp'], row['humidity'], row['wind_speed'], row['rain_type'], row['rain_1h']))

    conn.commit()
    cur.close()
    conn.close()
    print(f"✅ {len(data)}개 도시 데이터 DB 저장 완료")


# ── Task 4: Report ────────────────────────────────────────────────────
# 오늘 수집된 데이터 기반으로 일간 통계 저장
def report(**context):
    conn = psycopg2.connect(DB_CONN)
    cur = conn.cursor()

    today = datetime.now().date()

    for city in CITIES:
        cur.execute("""
            INSERT INTO weather_daily_stats (city, stat_date, avg_temp, max_temp, min_temp, avg_humidity)
            SELECT city, %s,
                   ROUND(AVG(temp_celsius), 2),
                   MAX(temp_celsius),
                   MIN(temp_celsius),
                   AVG(humidity)::int
            FROM weather_raw
            WHERE city = %s AND DATE(collected_at) = %s
            GROUP BY city
            ON CONFLICT (city, stat_date) DO UPDATE
              SET avg_temp     = EXCLUDED.avg_temp,
                  max_temp     = EXCLUDED.max_temp,
                  min_temp     = EXCLUDED.min_temp,
                  avg_humidity = EXCLUDED.avg_humidity
        """, (today, city['name'], today))

    conn.commit()
    cur.close()
    conn.close()
    print(f"✅ 일간 통계 저장 완료")


# ── Task 5: Notify ────────────────────────────────────────────────────
# Slack으로 결과 알림 (SLACK_WEBHOOK_URL 없으면 스킵)
def notify(**context):
    webhook_url = os.environ.get('SLACK_WEBHOOK_URL')
    if not webhook_url:
        print("⚠️ SLACK_WEBHOOK_URL 없음 - 알림 스킵")
        return

    data = context['ti'].xcom_pull(key='transformed_data')

    rain_label = {0: '없음', 1: '비', 2: '비/눈', 3: '눈', 5: '빗방울', 6: '빗방울/눈날림', 7: '눈날림'}
    message = "🌤 *오늘의 날씨 수집 완료*\n"
    for row in data:
        rain = rain_label.get(row['rain_type'], '알수없음')
        message += f"• {row['city']}: {row['temp']}℃, 습도 {row['humidity']}%, 강수 {rain}\n"

    requests.post(webhook_url, json={"text": message})
    print("✅ Slack 알림 전송 완료")


# ── DAG 정의 ──────────────────────────────────────────────────────────
with DAG(
    dag_id='weather_etl',
    default_args=default_args,
    start_date=datetime(2026, 1, 1),
    schedule='@hourly',     # 매시간 실행 (기상청 초단기실황은 1시간 단위)
    catchup=False,
    tags=['weather', 'etl'],
) as dag:

    t1 = PythonOperator(task_id='extract',   python_callable=extract)
    t2 = PythonOperator(task_id='transform', python_callable=transform)
    t3 = PythonOperator(task_id='load',      python_callable=load)
    t4 = PythonOperator(task_id='report',    python_callable=report)
    t5 = PythonOperator(task_id='notify',    python_callable=notify)

    t1 >> t2 >> t3 >> t4 >> t5