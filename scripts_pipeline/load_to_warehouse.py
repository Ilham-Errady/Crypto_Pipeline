import io
import os
import boto3
import sqlite3
import pandas as pd
import snowflake.connector
from snowflake.connector.pandas_tools import write_pandas
from dotenv import load_dotenv

# 1. شحن المتغيرات البيئية من ملف .env
load_dotenv()

TARGET = os.getenv("TARGET_WAREHOUSE", "SQLITE").upper()

# 2. إعداد الاتصال بـ Data Lake (MinIO) وقراءة طبقة Gold
s3_client = boto3.client(
    's3', 
    endpoint_url="http://minio:9000",
    aws_access_key_id="minioadmin",      
    aws_secret_access_key="minioadmin"   
)
GOLD_BUCKET = "crypto-gold"

def read_gold_table(name):
    print(f"📥 Attempting to fetch gold_{name}.parquet from MinIO...")
    obj = s3_client.get_object(Bucket=GOLD_BUCKET, Key=f"gold_{name}.parquet")
    return pd.read_parquet(io.BytesIO(obj['Body'].read()))

print("📖 Reading dimensional tables from Gold Layer...")
try:
    df_crypto = read_gold_table("dim_crypto")
    df_date = read_gold_table("dim_date_time")
    df_fact = read_gold_table("fact_crypto_metrics")
    print("✅ Successfully read all Parquet files from Gold Layer.")
except Exception as s3_err:
    print(f"❌ Error while reading from MinIO S3: {str(s3_err)}")
    raise s3_err

# تحويل التوقيت لنص لتفادي مشاكل التوافق ف الشحن
df_date['full_timestamp'] = df_date['full_timestamp'].astype(str)

# 3. الاتصال الديناميكي بالمستودع المحدد (Snowflake أو SQLite)
if TARGET == "SNOWFLAKE":
    print("☁️ Connecting to Cloud Data Warehouse: SNOWFLAKE...")
    print(f"🔗 Targeted Account Identifier: {os.getenv('SNOWFLAKE_ACCOUNT')}")
    
    try:
        conn = snowflake.connector.connect(
            user=os.getenv("SNOWFLAKE_USER"),
            password=os.getenv("SNOWFLAKE_PASSWORD"),
            account=os.getenv("SNOWFLAKE_ACCOUNT"),
            warehouse=os.getenv("SNOWFLAKE_WAREHOUSE"),
            database=os.getenv("SNOWFLAKE_DATABASE"),
            schema=os.getenv("SNOWFLAKE_SCHEMA"),
            login_timeout=15,         
            network_timeout=15         
        )
        cursor = conn.cursor()
        print("🔗 Successfully connected to Snowflake Cloud!")
    except Exception as conn_err:
        print(f"❌ Network or Credential Error during Snowflake connection: {str(conn_err)}")
        raise conn_err
else:
    # 👈 تعديل حاسم: إيلا مالقاش الاسم ف الـ .env، غايصاوب ملف سميتو crypto_warehouse.db تلقائياً
    db_name = os.getenv("DB_NAME", "crypto_warehouse.db")
    print(f"📦 Connecting to Local Data Warehouse: SQLITE ({db_name})...")
    conn = sqlite3.connect(db_name)
    cursor = conn.cursor()
    cursor.execute("PRAGMA foreign_keys = ON;")
try:
    # 🛠️ تعديل حاسم: تحويل أسماء الأعمدة إلى حروف كبيرة (UPPERCASE) لتتوافق تماماً مع Snowflake
    df_crypto.columns = [col.upper() for col in df_crypto.columns]
    df_date.columns = [col.upper() for col in df_date.columns]
    df_fact.columns = [col.upper() for col in df_fact.columns]

    print(f"📥 Ingesting records using {TARGET} strategy...")
    
    if TARGET == "SNOWFLAKE":
        print("🚀 Sending and creating tables dynamically in Snowflake Cloud...")
        # خاصية auto_create_table=True كتعطي لـ write_pandas الصلاحية تصاوب الجدول بـ الحروف الكبيرة آلياً
        write_pandas(conn, df_crypto, table_name='DIM_CRYPTO', auto_create_table=True, overwrite=True)
        print("✅ DIM_CRYPTO loaded into Snowflake.")
        write_pandas(conn, df_date, table_name='DIM_DATE_TIME', auto_create_table=True, overwrite=True)
        print("✅ DIM_DATE_TIME loaded into Snowflake.")
        write_pandas(conn, df_fact, table_name='FACT_CRYPTO_METRICS', auto_create_table=True, overwrite=True)
        print("✅ FACT_CRYPTO_METRICS loaded into Snowflake.")
    else:
        print("🚀 Creating tables structure for Local SQLite...")
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS DIM_CRYPTO (
                CRYPTO_KEY INT PRIMARY KEY, CRYPTO_ID TEXT, SYMBOL TEXT, NAME TEXT
            )
        """)
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS DIM_DATE_TIME (
                DATE_TIME_KEY INT PRIMARY KEY, FULL_TIMESTAMP TEXT,
                HOUR INT, DAY INT, MONTH INT, YEAR INT, DAY_OF_WEEK TEXT
            )
        """)
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS FACT_CRYPTO_METRICS (
                FACT_KEY INT PRIMARY KEY, CRYPTO_KEY INT, DATE_TIME_KEY INT,
                PRICE_USD REAL, MARKET_CAP_USD REAL, TOTAL_VOLUME_USD REAL, PRICE_CHANGE_24H_PCT REAL,
                FOREIGN KEY (CRYPTO_KEY) REFERENCES DIM_CRYPTO(CRYPTO_KEY),
                FOREIGN KEY (DATE_TIME_KEY) REFERENCES DIM_DATE_TIME(DATE_TIME_KEY)
            )
        """)
        df_crypto.to_sql('DIM_CRYPTO', conn, if_exists='append', index=False)
        df_date.to_sql('DIM_DATE_TIME', conn, if_exists='append', index=False)
        df_fact.to_sql('FACT_CRYPTO_METRICS', conn, if_exists='append', index=False)

    print("🪙 Dimensions and Fact tables loaded successfully.")

    # 4. الـ Validation والـ Data Quality Check للتحقق من وصول البيانات
    cursor.execute("SELECT COUNT(*) FROM FACT_CRYPTO_METRICS")
    total_rows = cursor.fetchone()[0]
    print(f"\n🔍 Validation Check: Total rows secured in {TARGET}: {total_rows}")
    print(f"🎉 Pipeline perfectly completed up to the {TARGET} Warehouse!")

except Exception as e:
    print(f"❌ Error occurred during warehouse loading: {str(e)}")
    raise e

finally:
    # إغلاق الاتصالات بشكل آمن ونظيف
    print("🔒 Closing database connections safely...")
    try:
        if TARGET == "SNOWFLAKE":
            cursor.close()
        conn.close()
        print("🔒 All connections closed.")
    except Exception as close_err:
        print(f"⚠️ Warning while closing connections: {str(close_err)}")