import json
import datetime
from io import BytesIO
import requests
from requests.exceptions import HTTPError, Timeout
from minio import Minio

# 1. إعدادات الاتصال بـ MinIO
MINIO_URL = "localhost:9000"
ACCESS_KEY = "minioadmin"
SECRET_KEY = "minioadmin"
BUCKET_NAME = "crypto-bronze"

minio_client = Minio(
    "minio:9000",  # 👈 رجعيها "minio:9000" نيشان بلا شريطة تحتانية
    access_key="minioadmin",
    secret_key="minioadmin",
    secure=False
)

if not minio_client.bucket_exists(BUCKET_NAME):
    minio_client.make_bucket(BUCKET_NAME)

def fetch_and_store_crypto():
    url = "https://api.coingecko.com/api/v3/coins/markets"
    params = {
        "vs_currency": "usd",
        "order": "market_cap_desc",
        "per_page": 10,
        "page": 1,
        "sparkline": "false"
    }
    
    try:
        print("Fetching data from CoinGecko API...")
        # 2. إدارة الـ Timeout (مثلاً 10 ثواني كحد أقصى للاستجابة)
        response = requests.get(url, params=params, timeout=10)
        
        # 3. إدارة أخطاء الـ HTTP (بحال 429 Too Many Requests أو 500 Server Error)
        response.raise_for_status()
        raw_data = response.json()
        
        # 4. حساب التاريخ والوقت لتشكيل المسار المنظم (Partitioning)
        now = datetime.datetime.utcnow()
        year = now.strftime("%Y")
        month = now.strftime("%m")
        day = now.strftime("%d")
        time_str = now.strftime("%H-%M-%S")
        
        payload = {
            "collected_at": now.isoformat(),
            "data": raw_data
        }
        
        json_bytes = json.dumps(payload, indent=4).encode('utf-8')
        json_stream = BytesIO(json_bytes)
        
        # تشكيل المسار المطلوب بالضبط: crypto-bronze/YYYY/MM/DD/raw_HH-MM-SS.json
        # (زدنا الوقت ف السمية غير باش يلا ركضتي الكود بزاف د المرات ف نفس النهار ما يتمسحش الملف القديم)
        object_name = f"{year}/{month}/{day}/raw_{time_str}.json"
        
        # 5. الرفع إلى MinIO
        minio_client.put_object(
            bucket_name=BUCKET_NAME,
            object_name=object_name,
            data=json_stream,
            length=len(json_bytes),
            content_type="application/json"
        )
        
        print(f"✅ Success! Stored in Bronze as: {object_name}")
        
    # إدارة استثناءات الأخطاء بالتفصيل
    except Timeout:
        print("❌ Error: The request timed out. CoinGecko API took too long to respond.")
    except HTTPError as http_err:
        print(f"❌ HTTP Error occurred: {http_err} (Check if API limit is reached: Code 429)")
    except Exception as e:
        print(f"❌ An unexpected error occurred: {str(e)}")

if __name__ == "__main__":
    fetch_and_store_crypto()