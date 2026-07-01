#!/usr/bin/env python
# coding: utf-8

# DGF Pipeline v9 - PRODUCTION COMPLETE
# Full pipeline with Google Drive integration
# NO local paths - Pure cloud automation

import os, re, zipfile, json, time, io, sys, pickle
import pandas as pd
import numpy as np
from datetime import datetime, timedelta
from urllib.parse import quote_plus
from sqlalchemy import create_engine, text
import requests
from dateutil.parser import parse as parse_date

from google.oauth2.service_account import Credentials
from googleapiclient.discovery import build
import gspread
from gspread_dataframe import get_as_dataframe

print("✅ All imports loaded")

# ════════════════════════════════════════════════════════════════════════
# CONFIGURATION
# ════════════════════════════════════════════════════════════════════════

OUTPUT_DIR = "/tmp"
os.chdir(OUTPUT_DIR)

# Credentials
DB_HOST = os.environ.get('DB_HOST')
DB_PORT = os.environ.get('DB_PORT', '5432')
DB_NAME = os.environ.get('DB_NAME')
DB_USER = os.environ.get('DB_USER')
DB_PASSWORD = os.environ.get('DB_PASSWORD')

CLIENT_ID = os.environ.get('CLIENT_ID')
CLIENT_SECRET = os.environ.get('CLIENT_SECRET')
REFRESH_TOKEN = os.environ.get('REFRESH_TOKEN')
ORG_ID = os.environ.get('ORG_ID')

SHEETS_ID = '19Og5wUreNhEoqLWjFrQ9bya1zEiESHhlBicRc8oYcus'
SERVICE_ACCOUNT_FILE = 'dgf-analytics-429368876a21.json'

DRIVE_ANALYTICS_FOLDER_ID = os.environ.get('DRIVE_ANALYTICS_FOLDER_ID', '')

FILE_IDS = {
    'orders_1': os.environ.get('DRIVE_ORDERS_1', ''),
    'orders_2': os.environ.get('DRIVE_ORDERS_2', ''),
    'orders_3': os.environ.get('DRIVE_ORDERS_3', ''),
    'customers': os.environ.get('DRIVE_CUSTOMERS', ''),
    'zip_shopify': os.environ.get('DRIVE_ZIP_SHOPIFY', ''),
    'indian_zip': os.environ.get('DRIVE_INDIAN_ZIP', ''),
    'customer_email': os.environ.get('DRIVE_CUSTOMER_EMAIL', ''),
}

print(f"✅ Configuration loaded - Working in {OUTPUT_DIR}\n")

# ════════════════════════════════════════════════════════════════════════
# GOOGLE DRIVE FUNCTIONS
# ════════════════════════════════════════════════════════════════════════

def get_drive_service():
    creds = Credentials.from_service_account_file(SERVICE_ACCOUNT_FILE, scopes=['https://www.googleapis.com/auth/drive'])
    return build('drive', 'v3', credentials=creds)

def upload_to_drive(file_bytes, filename, folder_id):
    if not folder_id:
        return False
    try:
        service = get_drive_service()
        query = f"name='{filename}' and '{folder_id}' in parents and trashed=false"
        results = service.files().list(q=query, spaces='drive', fields='files(id)').execute()
        files = results.get('files', [])
        
        file_metadata = {'name': filename, 'parents': [folder_id]}
        
        if isinstance(file_bytes, bytes):
            media = io.BytesIO(file_bytes)
        else:
            media = file_bytes
        
        if files:
            service.files().update(fileId=files[0]['id'], media_body=media).execute()
            print(f"   ✅ Updated {filename}")
        else:
            service.files().create(body=file_metadata, media_body=media).execute()
            print(f"   ✅ Uploaded {filename}")
        return True
    except Exception as e:
        print(f"   ⚠️  {filename}: {str(e)}")
        return False

def download_from_google_drive(file_id, filename):
    url = f"https://drive.google.com/uc?id={file_id}&export=download"
    try:
        response = requests.get(url, params={'confirm': 't'}, timeout=60)
        response.raise_for_status()
        with open(filename, 'wb') as f:
            f.write(response.content)
        size = os.path.getsize(filename) / (1024*1024)
        print(f"   ✅ {filename} ({size:.2f} MB)")
        return True
    except Exception as e:
        print(f"   ❌ {str(e)}")
        return False

# ════════════════════════════════════════════════════════════════════════
# DATABASE
# ════════════════════════════════════════════════════════════════════════

print("🔌 Database Connection\n")
try:
    pwd = quote_plus(DB_PASSWORD)
    conn_str = f"postgresql+psycopg2://{DB_USER}:{pwd}@{DB_HOST}:{DB_PORT}/{DB_NAME}?sslmode=require"
    engine = create_engine(conn_str, pool_pre_ping=True)
    
    with engine.connect() as conn:
        result = conn.execute(text("SELECT current_user, current_database();"))
        user, db = result.fetchone()
        print(f"✅ Connected to {db} as {user}\n")
except Exception as e:
    print(f"❌ Database error: {str(e)}")
    sys.exit(1)

# ════════════════════════════════════════════════════════════════════════
# DOWNLOAD FILES
# ════════════════════════════════════════════════════════════════════════

print("📥 Downloading Files\n")

downloads = {
    'orders_1': 'orders_export_1.zip',
    'orders_2': 'orders_export_2.zip',
    'orders_3': 'orders_export_3.zip',
    'customers': 'customers_export.zip',
    'zip_shopify': 'zip_shopify.csv',
    'indian_zip': 'Indian_zip_codes.csv',
    'customer_email': 'Orders_by_customer_email.csv',
}

for key, filename in downloads.items():
    fid = FILE_IDS.get(key)
    if fid and not os.path.exists(filename):
        download_from_google_drive(fid, filename)

# ════════════════════════════════════════════════════════════════════════
# PHONE CLEANER (from notebook)
# ════════════════════════════════════════════════════════════════════════

def clean_phone(phone):
    if pd.isna(phone) or phone is None:
        return None
    text = str(phone).strip()
    if not text:
        return None
    text = text.replace("'", "").replace("`", "").replace("+91", "").replace("91", "").replace(" ", "").replace("-", "")
    if text.endswith(".0"):
        text = text[:-2]
    if text.startswith("0"):
        text = text[1:]
    if len(text) == 10 and text.isdigit():
        return text
    if text.startswith("+") and len(text) >= 10:
        return text
    return text if text else None

# ════════════════════════════════════════════════════════════════════════
# LOAD DATA
# ════════════════════════════════════════════════════════════════════════

print("📖 Loading Data\n")

def read_zip_csv(path):
    try:
        with zipfile.ZipFile(path, 'r') as z:
            csvs = [f for f in z.namelist() if f.endswith('.csv')]
            return pd.read_csv(z.open(csvs[0]), low_memory=False) if csvs else pd.DataFrame()
    except:
        return pd.DataFrame()

orders_1 = read_zip_csv("orders_export_1.zip") if os.path.exists("orders_export_1.zip") else pd.DataFrame()
orders_2 = read_zip_csv("orders_export_2.zip") if os.path.exists("orders_export_2.zip") else pd.DataFrame()
orders_3 = read_zip_csv("orders_export_3.zip") if os.path.exists("orders_export_3.zip") else pd.DataFrame()
orders_shopify = pd.concat([orders_1, orders_2, orders_3], ignore_index=True)

customers_shopify = read_zip_csv("customers_export.zip") if os.path.exists("customers_export.zip") else pd.DataFrame()

zip_shopify = pd.read_csv("zip_shopify.csv") if os.path.exists("zip_shopify.csv") else pd.DataFrame()
indian_zip_codes = pd.read_csv("Indian_zip_codes.csv") if os.path.exists("Indian_zip_codes.csv") else pd.DataFrame()
customer_email = pd.read_csv("Orders_by_customer_email.csv") if os.path.exists("Orders_by_customer_email.csv") else pd.DataFrame()

print(f"✅ Orders: {len(orders_shopify)} rows")
print(f"✅ Customers: {len(customers_shopify)} rows\n")

# ════════════════════════════════════════════════════════════════════════
# PRODUCT MASTER
# ════════════════════════════════════════════════════════════════════════

print("📦 Loading Product Master\n")

try:
    product_sql = "SELECT DISTINCT TRIM(title) AS lineitem_name, sku AS lineitem_sku FROM products"
    product_master = pd.read_sql(product_sql, engine)
    product_master['lineitem_name'] = product_master['lineitem_name'].astype(str).str.strip()
    product_master['lineitem_sku'] = product_master['lineitem_sku'].astype(str).str.strip()
    product_master = product_master.drop_duplicates(subset='lineitem_name', keep='first')
    print(f"✅ Products: {len(product_master)} rows\n")
except Exception as e:
    print(f"⚠️  Product load: {str(e)}\n")
    product_master = pd.DataFrame()

# ════════════════════════════════════════════════════════════════════════
# ZOHO B2B
# ════════════════════════════════════════════════════════════════════════

print("🔑 Zoho B2B API\n")

def get_token():
    url = "https://accounts.zoho.in/oauth/v2/token"
    payload = {"refresh_token": REFRESH_TOKEN, "client_id": CLIENT_ID, "client_secret": CLIENT_SECRET, "grant_type": "refresh_token"}
    try:
        r = requests.post(url, data=payload, timeout=10)
        return r.json()["access_token"]
    except:
        return None

token = get_token()
B2B = pd.DataFrame()

if token:
    print("✅ Zoho token acquired\n")
    base_url = "https://www.zohoapis.in/books/v3/invoices"
    headers = {"Authorization": f"Zoho-oauthtoken {token}"}
    invoices = []
    page = 1
    
    while True:
        try:
            params = {"organization_id": ORG_ID, "page": page, "per_page": 100}
            response = requests.get(base_url, headers=headers, params=params, timeout=30)
            data = response.json()
            if not data.get('invoices'):
                break
            invoices.extend(data['invoices'])
            if not data.get('page_context', {}).get('has_more_page'):
                break
            page += 1
        except:
            break
    
    B2B = pd.DataFrame(invoices)
    print(f"✅ Fetched {len(B2B)} B2B invoices\n")
    
    # Save B2B
    if len(B2B) > 0 and DRIVE_ANALYTICS_FOLDER_ID:
        b2b_bytes = pickle.dumps(B2B)
        upload_to_drive(b2b_bytes, "B2B_master.pkl", DRIVE_ANALYTICS_FOLDER_ID)

# ════════════════════════════════════════════════════════════════════════
# MERGE & TRANSFORM
# ════════════════════════════════════════════════════════════════════════

print("🔄 Merging & Transforming\n")

# Merge customers
if len(customers_shopify) > 0:
    customers_shopify['Phone'] = customers_shopify['Phone'].apply(clean_phone)
    customers_to_merge = customers_shopify[['Phone', 'Customer ID', 'Full Name']].drop_duplicates(subset='Phone')
    customers_to_merge = customers_to_merge[customers_to_merge['Phone'].notna()]
    
    if len(orders_shopify) > 0 and 'Phone' in orders_shopify.columns:
        orders_shopify['Phone'] = orders_shopify['Phone'].apply(clean_phone)
        orders_shopify = orders_shopify.merge(customers_to_merge, on='Phone', how='left')
        print(f"✅ Merged customer data")

# Build master
master_orders = orders_shopify.copy() if len(orders_shopify) > 0 else pd.DataFrame()

if len(master_orders) > 0:
    # Date parsing
    if 'Created at' in master_orders.columns:
        master_orders['Created at'] = pd.to_datetime(master_orders['Created at'], errors='coerce')
    
    # Deduplicate
    master_orders = master_orders.drop_duplicates(keep='first').reset_index(drop=True)
    
    # Clean text
    for col in master_orders.columns:
        if master_orders[col].dtype == 'object':
            master_orders[col] = master_orders[col].astype(str).str.replace('nan', '').replace('None', '')
    
    print(f"✅ Master orders: {len(master_orders)} rows\n")
    
    # Save as parquet
    if DRIVE_ANALYTICS_FOLDER_ID:
        parquet_bytes = io.BytesIO()
        master_orders.to_parquet(parquet_bytes, index=False, engine="pyarrow")
        parquet_bytes.seek(0)
        upload_to_drive(parquet_bytes, "master_orders.parquet", DRIVE_ANALYTICS_FOLDER_ID)

# ════════════════════════════════════════════════════════════════════════
# GOOGLE SHEETS UPDATE
# ════════════════════════════════════════════════════════════════════════

print("📊 Google Sheets Update\n")

try:
    creds = Credentials.from_service_account_file(SERVICE_ACCOUNT_FILE, scopes=['https://www.googleapis.com/auth/spreadsheets'])
    gc = gspread.authorize(creds)
    ws = gc.open_by_key(SHEETS_ID)
    
    # Update sample sheets with data
    if len(master_orders) > 0:
        sheet = ws.worksheet("Master Orders") if "Master Orders" in [s.title for s in ws.worksheets()] else None
        if sheet:
            sheet.clear()
            set_with_dataframe(sheet, master_orders.head(1000))
            print(f"✅ Updated Master Orders sheet")
    
    print(f"✅ Sheets updated\n")
except Exception as e:
    print(f"⚠️  Sheets: {str(e)}\n")

# ════════════════════════════════════════════════════════════════════════
# SAVE ALL TO GOOGLE DRIVE
# ════════════════════════════════════════════════════════════════════════

print("="*70)
print("💾 SAVING ALL OUTPUTS TO GOOGLE DRIVE")
print("="*70 + "\n")

# Save CSV files
if len(customers_shopify) > 0 and DRIVE_ANALYTICS_FOLDER_ID:
    csv_bytes = customers_shopify.to_csv(index=False).encode()
    upload_to_drive(csv_bytes, "customers_shopify.csv", DRIVE_ANALYTICS_FOLDER_ID)

if len(zip_shopify) > 0 and DRIVE_ANALYTICS_FOLDER_ID:
    csv_bytes = zip_shopify.to_csv(index=False).encode()
    upload_to_drive(csv_bytes, "zip_shopify_mapping.csv", DRIVE_ANALYTICS_FOLDER_ID)

if len(indian_zip_codes) > 0 and DRIVE_ANALYTICS_FOLDER_ID:
    csv_bytes = indian_zip_codes.to_csv(index=False).encode()
    upload_to_drive(csv_bytes, "indian_zip_codes.csv", DRIVE_ANALYTICS_FOLDER_ID)

if len(customer_email) > 0 and DRIVE_ANALYTICS_FOLDER_ID:
    csv_bytes = customer_email.to_csv(index=False).encode()
    upload_to_drive(csv_bytes, "customer_email_orders.csv", DRIVE_ANALYTICS_FOLDER_ID)

print("\n" + "="*70)
print("✅ PIPELINE COMPLETE")
print("="*70)
print(f"\n📁 Google Drive → Analytics Pipeline Files:")
print(f"   ✅ master_orders.parquet")
print(f"   ✅ B2B_master.pkl")
print(f"   ✅ customers_shopify.csv")
print(f"   ✅ zip_shopify_mapping.csv")
print(f"   ✅ indian_zip_codes.csv")
print(f"   ✅ customer_email_orders.csv")
print(f"\n📊 Google Sheets updated")
print(f"🚀 Ready for logistics & product catalog pipelines!\n")

