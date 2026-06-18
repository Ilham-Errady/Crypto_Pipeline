import json
from io import BytesIO
import boto3
import pandas as pd

# 1. الاتصال بـ MinIO وصناعة صندوق الـ Gold
s3_client = boto3.client(
    's3', 
    endpoint_url="http://minio:9000",  # 👈 هادي هي اللي غاتخليهم يتواصلوا داخل Docker
    aws_access_key_id="minioadmin", 
    aws_secret_access_key="minioadmin"
)
GOLD_BUCKET = "crypto-gold"
s3_client.create_bucket(Bucket=GOLD_BUCKET) if GOLD_BUCKET not in [b['Name'] for b in s3_client.list_buckets()['Buckets']] else None

try:
    print("Reading from Silver...")
    # 2. قراءة أحدث ملف Parquet من الـ Silver
    objects = s3_client.list_objects_v2(Bucket="crypto-silver").get('Contents', [])
    latest_silver_key = max([obj for obj in objects if obj['Key'].endswith('.parquet')], key=lambda x: x['LastModified'])['Key']
    
    response = s3_client.get_object(Bucket="crypto-silver", Key=latest_silver_key)
    df_silver = pd.read_parquet(BytesIO(response['Body'].read()))

    # ----------------------------------------------------
    # 3. بناء الجداول على حساب الـ ERD بالضبط (Modélisation)
    # ----------------------------------------------------
    # أ. جدول الـ DIM_CRYPTO
    dim_crypto = df_silver[['crypto_id', 'symbol', 'name']].drop_duplicates().copy()
    dim_crypto['crypto_key'] = range(1, len(dim_crypto) + 1) # Surrogate Key (PK)
    dim_crypto = dim_crypto[['crypto_key', 'crypto_id', 'symbol', 'name']]

    # ب. جدول الـ DIM_DATE_TIME
    collected_time = df_silver['collected_at'].iloc[0]
    dim_date_time = pd.DataFrame([{
        'date_time_key': 1, # (PK)
        'full_timestamp': collected_time,
        'hour': collected_time.hour, 'day': collected_time.day,
        'month': collected_time.month, 'year': collected_time.year,
        'day_of_week': collected_time.strftime('%A')
    }])

    # ج. جدول الـ FACT_CRYPTO_METRICS (مع ضمان السلامة المرجعية)
    fact_crypto = df_silver.merge(dim_crypto, on='crypto_id', how='inner')
    fact_crypto['date_time_key'] = 1
    fact_crypto['fact_key'] = range(1, len(fact_crypto) + 1) # (PK)
    
    # الترتيب الصارم للأعمدة كما في الـ ERD
    fact_crypto = fact_crypto[[
        'fact_key', 'crypto_key', 'date_time_key', 
        'price_usd', 'market_cap_usd', 'total_volume_usd', 'price_change_24h_pct'
    ]]

    # ⚠️ التأكد من الـ Intégrité Référentielle (حتى شي مفتاح ما خاوي)
    assert not fact_crypto['crypto_key'].isna().any(), "FK Crypto non résolue!"

    # ----------------------------------------------------
    # 4. حفظ كل جدول فـ ملف Parquet بوحدو فـ الـ Gold
    # ----------------------------------------------------
    for name, df_gold in [("dim_crypto", dim_crypto), ("dim_date_time", dim_date_time), ("fact_crypto_metrics", fact_crypto)]:
        buf = BytesIO()
        df_gold.to_parquet(buf, index=False)
        s3_client.put_object(Bucket=GOLD_BUCKET, Key=f"gold_{name}.parquet", Body=buf.getvalue())
        print(f"🪙 Saved table: gold_{name}.parquet")

    print("\n🎉 الـ Pipeline كمل كامل بنجاح من الـ API تال الـ Gold!")

except Exception as e:
    print(f"❌ Error in Gold layer: {str(e)}")