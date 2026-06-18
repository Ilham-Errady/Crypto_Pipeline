import json
from io import BytesIO
import boto3
import pandas as pd

# ----------------------------------------------------
# 1. الاتصال بـ MinIO (سطور كلاسيكية ثابتة)
# ----------------------------------------------------
s3_client = boto3.client(
    's3', 
    endpoint_url="http://minio:9000",  # 👈 هادي هي اللي غاتخليهم يتواصلوا داخل Docker
    aws_access_key_id="minioadmin", 
    aws_secret_access_key="minioadmin"
)
s3_client.create_bucket(Bucket="crypto-silver") if "crypto-silver" not in [b['Name'] for b in s3_client.list_buckets()['Buckets']] else None

# ----------------------------------------------------
# 2. قراءة أحدث ملف JSON من الـ Bronze
# ----------------------------------------------------
# جلب كاع الملفات واختيار الأحدث
objects = s3_client.list_objects_v2(Bucket="crypto-bronze").get('Contents', [])
latest_key = max([obj for obj in objects if obj['Key'].endswith('.json')], key=lambda x: x['LastModified'])['Key']

# قراءة محتوى الملف وتحويله لـ Dictionary
response = s3_client.get_object(Bucket="crypto-bronze", Key=latest_key)
file_data = json.loads(response['Body'].read().decode('utf-8'))

# ----------------------------------------------------
# 3. التنظيف والتحويل لـ Parquet بـ Pandas
# ----------------------------------------------------
# تحويل الداتا لـ DataFrame وتغيير الأسماء لتطابق الـ ERD
df = pd.DataFrame(file_data["data"])
mapping = {'id': 'crypto_id', 'name': 'name', 'symbol': 'symbol', 'current_price': 'price_usd', 'market_cap': 'market_cap_usd', 'total_volume': 'total_volume_usd', 'price_change_percentage_24h': 'price_change_24h_pct'}
df = df[list(mapping.keys())].rename(columns=mapping)

# إضافة الوقت وتعديل الأنواع لـ FLOAT
df['collected_at'] = pd.to_datetime(file_data["collected_at"])
df['symbol'] = df['symbol'].str.upper()
df[['price_usd', 'market_cap_usd', 'total_volume_usd', 'price_change_24h_pct']] = df[['price_usd', 'market_cap_usd', 'total_volume_usd', 'price_change_24h_pct']].astype(float)

# تحويل لـ Parquet وحفظه فـ الـ Silver بنفس التقسيم التاريخي
parquet_buffer = BytesIO()
df.to_parquet(parquet_buffer, index=False, engine='pyarrow')

silver_key = latest_key.replace("raw_", "clean_").replace(".json", ".parquet")
s3_client.put_object(Bucket="crypto-silver", Key=silver_key, Body=parquet_buffer.getvalue())

print(f"✅ Done! Saved as: {silver_key}")