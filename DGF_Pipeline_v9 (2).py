#!/usr/bin/env python
# coding: utf-8
# ══════════════════════════════════════════════════════════════════════════
# DGF ANALYTICS PIPELINE v9 — GITHUB ACTIONS AUTOMATION EDITION
# ══════════════════════════════════════════════════════════════════════════
# Faithful reproduction of the notebook. ONLY data I/O changed:
#   • Static inputs downloaded from Google Drive (by file ID)
#   • B2B incremental sync (pkl + json) persisted to Google Drive
#   • master_orders.parquet saved to Google Drive (for logistics/catalogue)
#   • Service-account JSON written from a GitHub secret at runtime
#   • Saksham sheet push skipped; COGS + Master_orders sheet pushes kept
# ══════════════════════════════════════════════════════════════════════════

import os, re, json, time, io, sys, zipfile
import pandas as pd
import numpy as np
from datetime import datetime, timedelta, date
from urllib.parse import quote_plus
from sqlalchemy import create_engine, text
from dateutil.parser import parse as parse_date
import requests

import pygsheets
from google.oauth2.service_account import Credentials
from googleapiclient.discovery import build
from googleapiclient.http import MediaIoBaseDownload, MediaFileUpload
try:
    import gspread
    from gspread_dataframe import get_as_dataframe
except ImportError:
    pass

print("✅ Imports loaded")

# ══════════════════════════════════════════════════════════════════════════
# 0. ENVIRONMENT + CREDENTIALS  (cloud-safe; falls back to notebook values)
# ══════════════════════════════════════════════════════════════════════════
OUTPUT_DIR = "/tmp" if os.name != "nt" else r"D:\downloads"
os.makedirs(OUTPUT_DIR, exist_ok=True)
os.chdir(OUTPUT_DIR)

# --- Database
DB_HOST     = os.environ.get("DB_HOST",     "dgfdb.clsykici051f.ap-south-1.rds.amazonaws.com")
DB_PORT     = os.environ.get("DB_PORT",     "5432")
DB_NAME     = os.environ.get("DB_NAME",     "dgfdb")
DB_USER     = os.environ.get("DB_USER",     "readonly_user")
DB_PASSWORD = os.environ.get("DB_PASSWORD", "Gurugram@2026")

# --- Zoho
CLIENT_ID     = os.environ.get("CLIENT_ID",     "1000.SH8C19MA1Y8B5EFMLDIITO3NHOS7IP")
CLIENT_SECRET = os.environ.get("CLIENT_SECRET", "18fef8bd17c7844f9ba3329934fe8cdb4f7a0fe9b3")
REFRESH_TOKEN = os.environ.get("REFRESH_TOKEN", "1000.5bfe9f0da4afa9f1573fb7af36c43018.ccae5b792cf1827e40d8804f19c9f107")
ORG_ID        = os.environ.get("ORG_ID",        "60068564826")

# --- Google
SERVICE_ACCOUNT_FILE = "dgf-analytics-429368876a21.json"
SHEETS_ID            = "19Og5wUreNhEoqLWjFrQ9bya1zEiESHhlBicRc8oYcus"   # COGS workbook
DRIVE_FOLDER_ID      = os.environ.get("DRIVE_ANALYTICS_FOLDER_ID", "")   # Analytics Pipeline Files

# Write the service-account JSON from BASE64 secret
import base64
import base64

if not os.path.exists(SERVICE_ACCOUNT_FILE):
    sa_b64 = os.environ.get("GOOGLE_SERVICE_ACCOUNT_JSON_BASE64", "")
    if sa_b64:
        try:
            sa_json_str = base64.b64decode(sa_b64).decode('utf-8')
            with open(SERVICE_ACCOUNT_FILE, "w") as f:
                f.write(sa_json_str)
            print("✅ Service-account JSON written from base64 secret")
        except Exception as e:
            print(f"❌ Could not decode base64: {e}")
    else:
        print("⚠️  GOOGLE_SERVICE_ACCOUNT_JSON_BASE64 secret not set")

# Static input file IDs (Google Drive)
FILE_IDS = {
    "orders_export_1 (2).zip"          : os.environ.get("DRIVE_ORDERS_1", ""),
    "orders_export_2 (2).zip"          : os.environ.get("DRIVE_ORDERS_2", ""),
    "orders_export_3 (2).zip"          : os.environ.get("DRIVE_ORDERS_3", ""),
    "customers_export (2).zip"         : os.environ.get("DRIVE_CUSTOMERS", ""),
    "zip_shopify.csv"                  : os.environ.get("DRIVE_ZIP_SHOPIFY", ""),
    "Indian zip codes_modified.zip.csv": os.environ.get("DRIVE_INDIAN_ZIP", ""),
    "Orders by customer email (1).csv" : os.environ.get("DRIVE_CUSTOMER_EMAIL", ""),
}

print(f"✅ Working directory: {OUTPUT_DIR}")
print(f"✅ Drive folder set : {'yes' if DRIVE_FOLDER_ID else 'NO (uploads will be skipped)'}")

# ══════════════════════════════════════════════════════════════════════════
# GOOGLE DRIVE HELPERS  (service-account based — robust for private files)
# ══════════════════════════════════════════════════════════════════════════
_DRIVE_SCOPES = ["https://www.googleapis.com/auth/drive"]

def get_drive_service():
    creds = Credentials.from_service_account_file(SERVICE_ACCOUNT_FILE, scopes=_DRIVE_SCOPES)
    return build("drive", "v3", credentials=creds)

def drive_download_by_id(file_id, dest_path):
    """Download a Drive file (by ID) to dest_path using the service account."""
    try:
        service = get_drive_service()
        request = service.files().get_media(fileId=file_id)
        fh = io.FileIO(dest_path, "wb")
        downloader = MediaIoBaseDownload(fh, request)
        done = False
        while not done:
            _status, done = downloader.next_chunk()
        fh.close()
        mb = os.path.getsize(dest_path) / (1024 * 1024)
        print(f"   ✅ {os.path.basename(dest_path)} ({mb:.2f} MB)")
        return True
    except Exception as e:
        print(f"   ❌ Download failed for {os.path.basename(dest_path)}: {e}")
        return False

def drive_find_in_folder(filename, folder_id):
    service = get_drive_service()
    q = f"name = '{filename}' and '{folder_id}' in parents and trashed = false"
    res = service.files().list(q=q, spaces="drive", fields="files(id)").execute()
    files = res.get("files", [])
    return files[0]["id"] if files else None

def drive_download_from_folder(filename, folder_id, dest_path):
    """Download a file by NAME from a folder (used for B2B pkl + sync json)."""
    try:
        fid = drive_find_in_folder(filename, folder_id)
        if not fid:
            return False
        return drive_download_by_id(fid, dest_path)
    except Exception as e:
        print(f"   ⚠️  Could not fetch {filename} from Drive: {e}")
        return False

def drive_upload_to_folder(local_path, filename, folder_id):
    """Create or update a file in the Drive folder (last-write-wins)."""
    if not folder_id:
        print(f"   ⚠️  DRIVE_ANALYTICS_FOLDER_ID not set — skipping upload of {filename}")
        return False
    try:
        service = get_drive_service()
        existing = drive_find_in_folder(filename, folder_id)
        media = MediaFileUpload(local_path, resumable=True)
        if existing:
            service.files().update(fileId=existing, media_body=media).execute()
            print(f"   ✅ Updated on Drive: {filename}")
        else:
            service.files().create(
                body={"name": filename, "parents": [folder_id]},
                media_body=media,
            ).execute()
            print(f"   ✅ Uploaded to Drive: {filename}")
        return True
    except Exception as e:
        print(f"   ⚠️  Upload failed for {filename}: {e}")
        return False

# ══════════════════════════════════════════════════════════════════════════
# DOWNLOAD STATIC INPUTS FROM GOOGLE DRIVE
# ══════════════════════════════════════════════════════════════════════════
print("\n📥 Downloading static input files from Google Drive...")
for fname, fid in FILE_IDS.items():
    dest = os.path.join(OUTPUT_DIR, fname)
    if fid and not os.path.exists(dest):
        drive_download_by_id(fid, dest)
    elif not fid:
        print(f"   ⚠️  No file ID for {fname} — downstream read may fail")

# ══════════════════════════════════════════════════════════════════════════
# ZOHO OAUTH TOKEN (sanity)   [notebook cell 1]
# ══════════════════════════════════════════════════════════════════════════
def get_access_token():
    url = "https://accounts.zoho.in/oauth/v2/token"
    payload = {
        "refresh_token": REFRESH_TOKEN,
        "client_id": CLIENT_ID,
        "client_secret": CLIENT_SECRET,
        "grant_type": "refresh_token",
    }
    r = requests.post(url, data=payload, timeout=10)
    r.raise_for_status()
    return r.json()["access_token"]

token = get_access_token()
print("\n✅ Access token acquired")

# ══════════════════════════════════════════════════════════════════════════
# B2B FETCH FROM ZOHO API — INCREMENTAL   [notebook cell 2]
# pkl + sync-state now persisted to Google Drive (folder = DRIVE_FOLDER_ID)
# ══════════════════════════════════════════════════════════════════════════
MASTER_FILE     = os.path.join(OUTPUT_DIR, "B2B_master.pkl")
SYNC_STATE_FILE = os.path.join(OUTPUT_DIR, "B2B_sync_state.json")
FULL_LOAD_START = "2026-01-01"

# Pull previous state from Drive so incremental sync works across cloud runs
if DRIVE_FOLDER_ID:
    print("\n📥 Restoring B2B state from Drive (if present)...")
    drive_download_from_folder("B2B_master.pkl",     DRIVE_FOLDER_ID, MASTER_FILE)
    drive_download_from_folder("B2B_sync_state.json", DRIVE_FOLDER_ID, SYNC_STATE_FILE)

def get_oauth_token():
    print("🔑 Generating OAuth token...")
    url = "https://accounts.zoho.in/oauth/v2/token"
    payload = {"grant_type": "refresh_token", "client_id": CLIENT_ID,
               "client_secret": CLIENT_SECRET, "refresh_token": REFRESH_TOKEN}
    try:
        response = requests.post(url, data=payload, timeout=10)
        response.raise_for_status()
        data = response.json()
        if "access_token" in data:
            print("   ✅ Token generated successfully!")
            return data["access_token"]
        print(f"   ❌ No access token in response: {data}")
        return None
    except Exception as e:
        print(f"   ❌ Token generation failed: {e}")
        return None

RAW_COLUMNS = [
    'invoice_date','invoice_id','invoice_number','invoice_status',
    'customer_id','customer_name','customer_city','customer_state',
    'due_date','balance','invoice_total','invoice_subtotal','notes',
    'payment_terms_label','reference_number','place_of_supply',
    'shipping_attention','shipping_state',
    'line_item_id','item_name','item_description','quantity','unit',
    'item_price','item_total','discount_amount','tax_name',
    'tax_percentage','tax_amount','sku','hsn_sac','product_id',
]

GST_STATE = {
    'JK':'01-Jammu and Kashmir','HP':'02-Himachal Pradesh','PB':'03-Punjab',
    'CH':'04-Chandigarh','UT':'05-Uttarakhand','UK':'05-Uttarakhand',
    'HR':'06-Haryana','DL':'07-Delhi','RJ':'08-Rajasthan','UP':'09-Uttar Pradesh',
    'BR':'10-Bihar','SK':'11-Sikkim','AR':'12-Arunachal Pradesh','NL':'13-Nagaland',
    'MN':'14-Manipur','MZ':'15-Mizoram','TR':'16-Tripura','ML':'17-Meghalaya',
    'AS':'18-Assam','WB':'19-West Bengal','JH':'20-Jharkhand','OD':'21-Odisha',
    'OR':'21-Odisha','CG':'22-Chhattisgarh','MP':'23-Madhya Pradesh','GJ':'24-Gujarat',
    'DN':'26-Dadra and Nagar Haveli and Daman and Diu','DD':'26-Dadra and Nagar Haveli and Daman and Diu',
    'MH':'27-Maharashtra','KA':'29-Karnataka','GA':'30-Goa','LD':'31-Lakshadweep',
    'KL':'32-Kerala','TN':'33-Tamil Nadu','PY':'34-Puducherry',
    'AN':'35-Andaman and Nicobar Islands','TS':'36-Telangana','TG':'36-Telangana',
    'AP':'37-Andhra Pradesh','LA':'38-Ladakh',
}
GST_STATE_BY_NAME = {v.split('-',1)[1].lower(): v for v in set(GST_STATE.values())}

def load_sync_state():
    if os.path.exists(SYNC_STATE_FILE):
        try:
            with open(SYNC_STATE_FILE) as f:
                return json.load(f)
        except:
            return {"last_sync_time": None, "last_invoice_count": 0}
    return {"last_sync_time": None, "last_invoice_count": 0}

def save_sync_state(last_time, count):
    with open(SYNC_STATE_FILE, "w") as f:
        json.dump({"last_sync_time": last_time, "last_invoice_count": count,
                   "last_run": datetime.now().isoformat()}, f, indent=2)
    print("   ✅ Sync state saved")

def load_master():
    if os.path.exists(MASTER_FILE):
        try:
            df = pd.read_pickle(MASTER_FILE)
            print(f"   ✅ Loaded master: {len(df):,} rows")
            return df
        except:
            print("   ⚠️  Could not load master file")
            return None
    return None

def get_existing_invoice_ids(master_df):
    if master_df is None or len(master_df) == 0:
        return set()
    existing_ids = set(master_df['invoice_id'].unique())
    print(f"   📋 Already have: {len(existing_ids):,} invoices")
    return existing_ids

def _place_with_code(place_of_supply, shipping_state):
    if place_of_supply and str(place_of_supply).strip().upper() in GST_STATE:
        return GST_STATE[str(place_of_supply).strip().upper()]
    if shipping_state and str(shipping_state).strip().lower() in GST_STATE_BY_NAME:
        return GST_STATE_BY_NAME[str(shipping_state).strip().lower()]
    return f"00-{shipping_state}" if shipping_state else None

def get_invoice_ids(token, sync_date=None, date_start=FULL_LOAD_START):
    headers = {"Authorization": f"Zoho-oauthtoken {token}"}
    page, all_ids = 1, []
    if sync_date:
        try:
            dt = parse_date(sync_date) - timedelta(days=1)
            fetch_from = dt.strftime('%Y-%m-%d')
            print(f"   📅 Fetching IDs from {fetch_from} onwards...")
        except:
            fetch_from = date_start
    else:
        fetch_from = date_start
    while True:
        params = {"organization_id": ORG_ID, "page": page, "per_page": 200,
                  "filter_by": "Status.All", "date_start": fetch_from}
        try:
            r = requests.get("https://www.zohoapis.in/books/v3/invoices",
                             headers=headers, params=params, timeout=15)
            if r.status_code == 401:
                print("   ❌ 401 Error: Authentication failed. Token may be expired."); break
            if r.status_code == 400:
                print(f"   ❌ 400 error on page {page}"); break
            r.raise_for_status()
            data = r.json()
            if data.get("code") != 0:
                print(f"   ❌ Zoho error: {data.get('message')}"); break
            invoices = data.get("invoices", [])
            if not invoices: break
            all_ids.extend(x["invoice_id"] for x in invoices)
            if page % 5 == 0 or len(invoices) < 200:
                print(f"      📦 Page {page}: {len(invoices)} IDs (total: {len(all_ids)})")
            page += 1
            time.sleep(0.3)
        except Exception as e:
            print(f"   ⚠️  Page {page} error: {e}"); break
    return all_ids

def get_invoice_detail(invoice_id, token, retries=3):
    headers = {"Authorization": f"Zoho-oauthtoken {token}"}
    url = f"https://www.zohoapis.in/books/v3/invoices/{invoice_id}"
    for attempt in range(retries):
        try:
            r = requests.get(url, headers=headers, params={"organization_id": ORG_ID}, timeout=15)
            r.raise_for_status()
            data = r.json()
            if data.get("code") != 0:
                raise Exception(data.get("message"))
            return data.get("invoice")
        except Exception:
            if attempt < retries - 1:
                time.sleep(2 ** attempt)
            else:
                return None

def flatten_invoice_to_rows(inv):
    if not inv:
        return []
    billing  = inv.get("billing_address", {}) or {}
    shipping = inv.get("shipping_address", {}) or {}
    items = inv.get("line_items", []) or [{}]
    rows = []
    for idx, it in enumerate(items):
        rows.append({
            'invoice_date': inv.get("date"), 'invoice_id': inv.get("invoice_id"),
            'invoice_number': inv.get("invoice_number"), 'invoice_status': inv.get("status"),
            'customer_id': inv.get("customer_id"), 'customer_name': inv.get("customer_name"),
            'customer_city': billing.get("city"), 'customer_state': billing.get("state"),
            'due_date': inv.get("due_date"), 'balance': inv.get("balance"),
            'invoice_total': inv.get("total"), 'invoice_subtotal': inv.get("sub_total"),
            'notes': inv.get("notes"), 'payment_terms_label': inv.get("payment_terms_label"),
            'reference_number': inv.get("reference_number"), 'place_of_supply': inv.get("place_of_supply"),
            'shipping_attention': shipping.get("attention"), 'shipping_state': shipping.get("state"),
            'line_item_id': it.get("line_item_id", f"{inv.get('invoice_id')}_{idx}"),
            'item_name': it.get("name"), 'item_description': it.get("description"),
            'quantity': it.get("quantity"), 'unit': it.get("unit"),
            'item_price': it.get("rate"), 'item_total': it.get("item_total"),
            'discount_amount': it.get("discount_amount"), 'tax_name': it.get("tax_name"),
            'tax_percentage': it.get("tax_percentage"), 'tax_amount': it.get("tax_amount"),
            'sku': it.get("sku"), 'hsn_sac': it.get("hsn_or_sac"), 'product_id': it.get("item_id"),
        })
    return rows

INVOICE_COLUMNS = ['Invoice Date','Invoice ID','Invoice Number','Issued Date','Invoice Status','Customer ID','Customer Name','Place of Supply','Place of Supply(With State Code)','GST Treatment','Is Inclusive Tax','Is Export Without LUT/Bond','Tax Collected From Customer','Due Date','PurchaseOrder','Currency Code','Exchange Rate','Discount Type','Is Discount Before Tax','Template Name','Entity Discount Percent','TCS Tax Name','TCS Percentage','TDS Calculation Type','TDS Name','TDS Percentage','TDS Section Code','TDS Section','TDS Amount','SubTotal','Total','TotalRetentionAmountFCY','TotalRetentionAmountBCY','Balance','Adjustment','Adjustment Description','Expected Payment Date','Last Payment Date','Payment Terms','Payment Terms Label','Notes','Terms & Conditions','E-WayBill Number','E-WayBill Generated Time','E-WayBill Status','E-WayBill Cancelled Time','E-WayBill Expired Time','Transporter Name','Transporter ID','TCS Amount','Invoice Type','Entity Discount Amount','Shipping Charge','Shipping Charge Tax ID','Shipping Charge Tax Amount','Shipping Charge Tax Name','Shipping Charge Tax %','Shipping Charge Tax Type','Shipping Charge Tax Exemption Code','Shipping Charge SAC Code','Item Name','Item Desc','Quantity','Discount','Discount Amount','Item Total','Usage unit','Item Price','Product ID','Brand','Sales Order Number','Expense Reference ID','Recurrence Name','PayPal','Authorize.Net','Google Checkout','Payflow Pro','Stripe','Paytm','2Checkout','Braintree','Forte','WorldPay','Payments Pro','Square','WePay','Razorpay','ICICI EazyPay','GoCardless','Partial Payments','Billing Attention','Billing Address','Billing Street2','Billing City','Billing State','Billing Country','Billing Code','Billing Phone','Billing Fax','Shipping Attention','Shipping Address','Shipping Street2','Shipping City','Shipping State','Shipping Country','Shipping Code','Shipping Fax','Shipping Phone Number','Supplier Org Name','Supplier GST Registration Number','Supplier Street Address','Supplier City','Supplier State','Supplier Country','Supplier ZipCode','Supplier Phone','Supplier E-Mail','CGST Rate %','SGST Rate %','IGST Rate %','CESS Rate %','CGST(FCY)','SGST(FCY)','IGST(FCY)','CESS(FCY)','CGST','SGST','IGST','CESS','Reverse Charge Tax Name','Reverse Charge Tax Rate','Reverse Charge Tax Type','Item TDS Name','Item TDS Percentage','Item TDS Amount','Item TDS Section Code','Item TDS Section','GST Identification Number (GSTIN)','Nature Of Collection','SKU','Project ID','Project Name','HSN/SAC','Round Off','Sales person','Subject','Primary Contact EmailID','Primary Contact Mobile','Primary Contact Phone','Estimate Number','Item Type','Custom Charges','Shipping Bill#','Shipping Bill Date','Shipping Bill Total','PortCode','Reference Invoice#','Reference Invoice Date','Reference Invoice Type','GST Registration Number(Reference Invoice)','Reason for issuing Debit Note','E-Commerce Operator Name','E-Commerce Operator GSTIN','Account','Account Code','Supply Type','Tax ID','Item Tax','Item Tax %','Item Tax Amount','Item Tax Type','Item Tax Exemption Reason','Kit Combo Item Name']

def to_invoice_format(raw):
    out = pd.DataFrame(index=raw.index, columns=INVOICE_COLUMNS)
    direct = {
        'invoice_date':'Invoice Date','invoice_id':'Invoice ID','invoice_number':'Invoice Number',
        'invoice_status':'Invoice Status','customer_id':'Customer ID','customer_name':'Customer Name',
        'due_date':'Due Date','balance':'Balance','invoice_total':'Total','invoice_subtotal':'SubTotal',
        'notes':'Notes','payment_terms_label':'Payment Terms Label','reference_number':'PurchaseOrder',
        'item_name':'Item Name','item_description':'Item Desc','quantity':'Quantity','unit':'Usage unit',
        'item_price':'Item Price','item_total':'Item Total','discount_amount':'Discount Amount',
        'tax_name':'Item Tax','tax_percentage':'Item Tax %','tax_amount':'Item Tax Amount',
        'sku':'SKU','hsn_sac':'HSN/SAC','product_id':'Product ID','place_of_supply':'Place of Supply',
        'shipping_attention':'Shipping Attention','shipping_state':'Shipping State',
        'customer_city':'Shipping City',
    }
    for s, d in direct.items():
        if s in raw.columns:
            out[d] = raw[s].values
    out['Place of Supply(With State Code)'] = [
        _place_with_code(p, s) for p, s in zip(raw.get('place_of_supply', [pd.NA]*len(raw)),
                                               raw.get('shipping_state', [pd.NA]*len(raw)))
    ]
    out['Currency Code'] = 'INR'
    out['Exchange Rate'] = 1.0
    out['Invoice Type']  = 'Invoice'
    return out[INVOICE_COLUMNS]

print("\n" + "="*70)
print("🚀 B2B ZOHO FETCH - OPTIMIZED INCREMENTAL")
print("="*70)

print("\n[0] Generating OAuth token...")
token = get_oauth_token()
if not token:
    print("\n❌ FATAL: Could not generate OAuth token!"); sys.exit(1)

print("\n[1] Loading existing data...")
master = load_master()
existing_ids = get_existing_invoice_ids(master)
sync_state = load_sync_state()

print("\n[2] Fetching ALL invoice IDs from Zoho...")
all_ids = get_invoice_ids(token, sync_date=sync_state.get("last_sync_time"))
print(f"   📊 Total in Zoho: {len(all_ids):,}")

new_ids = [i for i in all_ids if i not in existing_ids]
print(f"\n[3] Smart filtering:")
print(f"   ✅ Already have: {len(existing_ids):,} invoices")
print(f"   🆕 New to fetch: {len(new_ids):,} invoices")

if len(new_ids) == 0:
    print("\n   ℹ️  No new invoices. Using existing master.")
    raw = master if master is not None else pd.DataFrame(columns=RAW_COLUMNS)
    B2B = to_invoice_format(raw)
else:
    print(f"\n[4] Fetching {len(new_ids):,} new invoices...")
    new_rows = []
    for i, inv_id in enumerate(new_ids, 1):
        if i % 50 == 0 or i == len(new_ids):
            print(f"      ⏳ {i}/{len(new_ids)} ({i/len(new_ids)*100:.0f}%)")
        inv = get_invoice_detail(inv_id, token)
        if inv:
            new_rows.extend(flatten_invoice_to_rows(inv))
    new_raw = pd.DataFrame(new_rows, columns=RAW_COLUMNS) if new_rows else pd.DataFrame(columns=RAW_COLUMNS)
    print(f"   ✅ Fetched: {len(new_raw):,} new line items")

    print("\n[5] Merging with existing master...")
    raw = pd.concat([master, new_raw], ignore_index=True) if master is not None else new_raw
    raw['invoice_date'] = pd.to_datetime(raw['invoice_date'], errors='coerce')
    raw['due_date']     = pd.to_datetime(raw['due_date'], errors='coerce')
    raw = (raw.sort_values('invoice_date', ascending=False)
              .drop_duplicates(subset=['invoice_id','line_item_id'], keep='first')
              .reset_index(drop=True))
    for c in ['quantity','item_price','item_total','discount_amount','tax_percentage',
              'tax_amount','balance','invoice_total','invoice_subtotal']:
        raw[c] = pd.to_numeric(raw[c], errors='coerce')

    raw.to_pickle(MASTER_FILE)
    print(f"   ✅ Master saved: {len(raw):,} line items ({raw['invoice_id'].nunique():,} invoices)")
    save_sync_state(datetime.now().isoformat(), raw['invoice_id'].nunique())

    # Persist B2B state back to Drive
    if DRIVE_FOLDER_ID:
        drive_upload_to_folder(MASTER_FILE, "B2B_master.pkl", DRIVE_FOLDER_ID)
        drive_upload_to_folder(SYNC_STATE_FILE, "B2B_sync_state.json", DRIVE_FOLDER_ID)

    B2B = to_invoice_format(raw)

print(f"\n[6] ✅ B2B READY: {B2B.shape[0]:,} rows × {B2B.shape[1]} columns")
print(f"    💰 Revenue: ₹{pd.to_numeric(B2B['Item Total'], errors='coerce').sum():,.0f}")

# Sanity check  [notebook cell 3]
_need = ['Invoice Date','Invoice Number','Customer Name','Place of Supply(With State Code)',
         'PurchaseOrder','Item Name','Quantity','Item Total','Usage unit','Item Price',
         'Shipping Attention','Shipping State','Invoice Status']
_missing = [c for c in _need if c not in B2B.columns]
print("✅ All required B2B columns present" if not _missing else f"❌ MISSING: {_missing}")

# ══════════════════════════════════════════════════════════════════════════
# DATABASE CONNECTION   [notebook cell 4]
# ══════════════════════════════════════════════════════════════════════════
conn_str = (
    f"postgresql+psycopg2://{DB_USER}:{quote_plus(DB_PASSWORD)}"
    f"@{DB_HOST}:{DB_PORT}/{DB_NAME}?sslmode=require"
)
engine = create_engine(conn_str)
with engine.connect() as conn:
    result = conn.execute(text("SELECT current_user, current_database();"))
    print("\n✅ DB connected:", result.fetchone())

# ══════════════════════════════════════════════════════════════════════════
# COGS DAY-ON-DAY PIPELINE → GOOGLE SHEETS (3 tabs)   [notebook cell 5]
# ══════════════════════════════════════════════════════════════════════════
_COGS_SCOPES = ['https://www.googleapis.com/auth/spreadsheets']

def convert_to_serializable(obj):
    if isinstance(obj, pd.Timestamp):
        return str(obj.date())
    elif isinstance(obj, np.datetime64):
        return str(obj)
    elif isinstance(obj, datetime):
        return obj.strftime('%Y-%m-%d %H:%M:%S')
    elif pd.isna(obj) or obj is None:
        return ''
    else:
        return str(obj)

def push_to_sheet(df, sheet_id, tab_name):
    print(f"   🔄 Converting {tab_name} to JSON-safe format...")
    df_copy = df.copy()
    for col in df_copy.columns:
        df_copy[col] = df_copy[col].apply(convert_to_serializable)
    creds = Credentials.from_service_account_file(SERVICE_ACCOUNT_FILE, scopes=_COGS_SCOPES)
    service = build('sheets', 'v4', credentials=creds)
    meta = service.spreadsheets().get(spreadsheetId=sheet_id).execute()
    sheet_names = [s['properties']['title'] for s in meta['sheets']]
    if tab_name not in sheet_names:
        service.spreadsheets().batchUpdate(
            spreadsheetId=sheet_id,
            body={'requests': [{'addSheet': {'properties': {'title': tab_name}}}]}
        ).execute()
        print(f"      ✅ Created tab: {tab_name}")
    service.spreadsheets().values().clear(spreadsheetId=sheet_id, range=f"'{tab_name}'!A1:ZZ").execute()
    values = [df_copy.columns.tolist()] + df_copy.values.tolist()
    service.spreadsheets().values().update(
        spreadsheetId=sheet_id, range=f"'{tab_name}'!A1",
        valueInputOption='RAW', body={'values': values}
    ).execute()
    print(f"   ✅ {len(df_copy):,} rows pushed to {tab_name}!")

def get_full_dod_query(start_date, end_date):
    return f"""
WITH order_totals AS (
    SELECT oi.order_id, SUM(oi.price * oi.item_quantity) AS order_gross
    FROM public.order_items oi
    INNER JOIN public.orders o ON o.order_id = oi.order_id AND o.status = true
        AND (o.payment_status = 1 OR o.payment_mode IN ('COD','POD') OR o.is_draft_order = true)
    WHERE oi.status = true
      AND (o.order_date AT TIME ZONE 'UTC' AT TIME ZONE 'Asia/Kolkata')::date
          BETWEEN '{start_date}'::date AND '{end_date}'::date
    GROUP BY oi.order_id
),
sold_items AS (
    SELECT
        (o.order_date AT TIME ZONE 'UTC' AT TIME ZONE 'Asia/Kolkata')::date AS order_date,
        p.product_id, p.title AS item_name, p.sku, p.quantity AS product_qty, u.unit AS unit_name,
        CASE WHEN c.is_btob = true THEN 'B2B' ELSE 'B2C' END AS channel,
        CASE WHEN o.payment_status = 0 THEN 'Pending' WHEN o.payment_status = 1 THEN 'Success' ELSE 'Unknown' END AS payment_status,
        CASE WHEN o.order_status = 0 THEN 'Unfulfilled' WHEN o.order_status = 1 THEN 'Fulfilled' WHEN o.order_status = 2 THEN 'Cancelled' ELSE 'Unknown' END AS fulfillment_status,
        SUM(oi.item_quantity) AS total_items_sold,
        SUM(((oi.price * oi.item_quantity) - ((oi.price * oi.item_quantity)/NULLIF(ot.order_gross,0)*COALESCE(o.discount_amount,0)) + COALESCE(oi.delivery_charge,0))) AS total_selling_price
    FROM public.orders o
    JOIN public.order_items oi ON oi.order_id = o.order_id AND oi.status = true
    JOIN order_totals ot ON ot.order_id = oi.order_id
    JOIN public.products p ON oi.item_id = p.product_id
    LEFT JOIN public.unit u ON p.unit_id = u.unit_id
    LEFT JOIN public.customers c ON o.customer_id = c.customer_id
    WHERE o.status = true
      AND (o.payment_status = 1 OR o.payment_mode IN ('COD','POD') OR o.is_draft_order = true)
      AND (o.order_date AT TIME ZONE 'UTC' AT TIME ZONE 'Asia/Kolkata')::date
          BETWEEN '{start_date}'::date AND '{end_date}'::date
    GROUP BY o.order_date, p.product_id, p.title, p.sku, p.quantity, u.unit, c.is_btob, o.payment_status, o.order_status
),
pc_buying_cost AS (
    SELECT
        (o.order_date AT TIME ZONE 'UTC' AT TIME ZONE 'Asia/Kolkata')::date AS order_date,
        oi.item_id AS product_id,
        CASE WHEN c.is_btob = true THEN 'B2B' ELSE 'B2C' END AS channel,
        CASE WHEN o.payment_status = 0 THEN 'Pending' WHEN o.payment_status = 1 THEN 'Success' ELSE 'Unknown' END AS payment_status,
        CASE WHEN o.order_status = 0 THEN 'Unfulfilled' WHEN o.order_status = 1 THEN 'Fulfilled' WHEN o.order_status = 2 THEN 'Cancelled' ELSE 'Unknown' END AS fulfillment_status,
        SUM(oi.item_quantity::numeric * COALESCE(recipe_cost.cost_per_unit, 0)) AS total_buying_price
    FROM public.order_items oi
    JOIN public.orders o ON o.order_id = oi.order_id AND o.status = true
    LEFT JOIN public.customers c ON o.customer_id = c.customer_id
    LEFT JOIN LATERAL (
        SELECT SUM(recipe.recipe_qty * COALESCE(batch_price.avg_unit_price, fallback_price.avg_unit_price, 0)) AS cost_per_unit
        FROM (
            SELECT DISTINCT ON (wfi.raw_id) wfi.raw_id, wfi.quantity::numeric AS recipe_qty
            FROM public.work_flow_output wfo
            JOIN public.work_flow_input wfi ON wfi.workflow_id = wfo.workflow_id AND wfi.status = true
            WHERE wfo.product_id = oi.item_id AND wfo.status = true
        ) recipe
        LEFT JOIN LATERAL (
            SELECT CASE WHEN SUM(b.consumed_qty) > 0 THEN SUM(b.consumed_qty * gi.unit_price)/SUM(b.consumed_qty) ELSE NULL END AS avg_unit_price
            FROM public.order_item_raw_batch_usage b
            JOIN public.grn_items gi ON gi.grn_item_id = b.grn_item_id AND gi.status = true
            WHERE b.order_item_id = oi.id AND b.raw_id = recipe.raw_id AND b.status = true
        ) batch_price ON true
        LEFT JOIN LATERAL (
            SELECT AVG(gi.unit_price)::numeric AS avg_unit_price
            FROM public.grn_items gi
            WHERE gi.rawitem_id = recipe.raw_id AND gi.status = true AND gi.unit_price > 0
        ) fallback_price ON batch_price.avg_unit_price IS NULL
    ) recipe_cost ON true
    WHERE oi.status = true
      AND (o.payment_status = 1 OR o.payment_mode IN ('COD','POD') OR o.is_draft_order = true)
      AND (o.order_date AT TIME ZONE 'UTC' AT TIME ZONE 'Asia/Kolkata')::date
          BETWEEN '{start_date}'::date AND '{end_date}'::date
    GROUP BY o.order_date, oi.item_id, c.is_btob, o.payment_status, o.order_status
)
SELECT
    si.order_date, si.product_id, si.item_name, si.sku, si.channel,
    si.payment_status, si.fulfillment_status, si.total_items_sold, si.unit_name,
    ROUND((si.total_items_sold::numeric / CASE WHEN LOWER(COALESCE(si.unit_name,'')) IN ('gm','gram','grams','g') THEN 1000 ELSE 1 END) * si.product_qty, 3) AS total_kgs_sold,
    ROUND(si.total_selling_price::numeric,2) AS total_selling_price,
    ROUND(COALESCE(pc.total_buying_price,0)::numeric,2) AS total_buying_price,
    ROUND((si.total_selling_price - COALESCE(pc.total_buying_price,0))::numeric,2) AS total_margin,
    CASE WHEN si.total_selling_price > 0 THEN ROUND(((si.total_selling_price - COALESCE(pc.total_buying_price,0))/si.total_selling_price*100)::numeric,2) ELSE 0 END AS total_margin_percent
FROM sold_items si
LEFT JOIN pc_buying_cost pc
    ON pc.product_id = si.product_id AND pc.order_date = si.order_date
    AND pc.channel = si.channel AND pc.payment_status = si.payment_status
    AND pc.fulfillment_status = si.fulfillment_status
ORDER BY si.order_date DESC, si.total_selling_price DESC
"""

print("\n" + "="*70)
print("✅ COGS DAY-ON-DAY PIPELINE")
print("="*70)
cogs_start = '2026-01-01'
cogs_end   = date.today().strftime('%Y-%m-%d')
try:
    df_all = pd.read_sql(get_full_dod_query(cogs_start, cogs_end), engine)
    print(f"   ✅ {len(df_all):,} rows fetched")
    push_to_sheet(df_all, SHEETS_ID, 'COGS_Daily_Full')
    daily_summary = df_all.groupby(['order_date', 'channel']).agg({
        'total_selling_price':'sum','total_buying_price':'sum',
        'total_margin':'sum','total_items_sold':'sum'
    }).reset_index().round(2).sort_values('order_date', ascending=False)
    push_to_sheet(daily_summary, SHEETS_ID, 'COGS_Daily_Summary')
    df_b2b = df_all[df_all['channel'] == 'B2B'].copy()
    push_to_sheet(df_b2b, SHEETS_ID, 'COGS_B2B_Only')
    print("   ✅ COGS pipeline complete (3 tabs)")
except Exception as e:
    print(f"   ❌ COGS ERROR: {e}")
    engine.dispose()
    engine = create_engine(conn_str)

# ══════════════════════════════════════════════════════════════════════════
# LOAD ADMIN DATA FROM DB   [notebook cell 6]
# ══════════════════════════════════════════════════════════════════════════
START_DATE = '2025-12-01'
admin_query = f"""
WITH order_totals AS (
    SELECT oi.order_id, SUM(oi.price * oi.item_quantity) AS order_gross
    FROM public.order_items oi
    INNER JOIN public.orders o ON o.order_id = oi.order_id AND o.status = true
        AND (o.payment_status = 1 OR o.payment_mode IN ('COD','POD') OR o.is_draft_order = true)
    WHERE oi.status = true
        AND (o.order_date AT TIME ZONE 'UTC' AT TIME ZONE 'Asia/Kolkata')::date >= '{START_DATE}'::date
    GROUP BY oi.order_id
)
SELECT
    TO_CHAR(o.created_date AT TIME ZONE 'UTC' AT TIME ZONE 'Asia/Kolkata','YYYY-MM-DD HH24:MI:SS') AS order_created_date,
    o.order_id,
    TO_CHAR(oi.exp_delivery_datetime AT TIME ZONE 'UTC' AT TIME ZONE 'Asia/Kolkata','YYYY-MM-DD') AS delivery_date,
    (CASE WHEN pws.from_time IS NOT NULL AND pws.to_time IS NOT NULL THEN pws.from_time::TEXT || ' - ' || pws.to_time::TEXT ELSE pws.slot_name END) AS delivery_time_slot,
    COALESCE(c.customer_name,'Unknown') AS customer_name, ca.address AS customer_address,
    c.mobile_no AS customer_contact, p.title AS product_title, o.note AS notes,
    (CASE WHEN o.is_draft_order = true THEN 'draftorder' ELSE o.channel END) AS sales_channel,
    (CASE WHEN o.payment_status = 0 THEN 'Pending' WHEN o.payment_status = 1 THEN 'Success' ELSE 'Unknown' END) AS payment_status,
    o.payment_mode,
    (CASE WHEN o.order_status = 0 THEN 'Unfulfilled' WHEN o.order_status = 1 THEN 'Fulfilled' WHEN o.order_status = 2 THEN 'Cancelled' ELSE 'Unknown' END) AS fulfillment_status_code,
    oi.item_quantity AS net_items_sold, oi.is_express_delivery, store.store_name,
    ROUND((oi.price * oi.item_quantity),2)::NUMERIC(12,2) AS gross_sales,
    o.discount_code AS discount_coupon_used,
    ROUND((oi.price * oi.item_quantity)/NULLIF(ot.order_gross,0)*COALESCE(o.discount_amount,0),2)::NUMERIC(12,2) AS discounts,
    ROUND((oi.price * oi.item_quantity)-((oi.price * oi.item_quantity)/NULLIF(ot.order_gross,0)*COALESCE(o.discount_amount,0)),2)::NUMERIC(12,2) AS net_sales,
    ROUND(COALESCE(oi.gst_amount,0),2)::NUMERIC(12,2) AS taxes,
    ROUND(COALESCE(oi.delivery_charge,0),2)::NUMERIC(12,2) AS shipping_charges,
    ROUND(((oi.price * oi.item_quantity)-((oi.price * oi.item_quantity)/NULLIF(ot.order_gross,0)*COALESCE(o.discount_amount,0))+COALESCE(oi.delivery_charge,0)),2)::NUMERIC(12,2) AS total_sales
FROM public.orders o
INNER JOIN public.order_items oi ON oi.order_id = o.order_id
INNER JOIN order_totals ot ON ot.order_id = oi.order_id
INNER JOIN public."darkStore_packagingStore" store ON store.id = oi.store_id
LEFT JOIN public.customers c ON o.customer_id = c.customer_id
LEFT JOIN public.customer_addresses ca ON o.address_id = ca.address_id
LEFT JOIN public.products p ON oi.item_id = p.product_id
LEFT JOIN public.pincode_wise_slot pws ON pws.id = oi.slot_id
WHERE o.status = true
    AND (o.payment_status = 1 OR o.payment_mode IN ('COD','POD') OR o.is_draft_order = true)
    AND oi.status = true
    AND (o.order_date AT TIME ZONE 'UTC' AT TIME ZONE 'Asia/Kolkata')::date >= '{START_DATE}'::date
ORDER BY o.order_id, oi.id
"""
admin = pd.read_sql(text(admin_query), engine)
print(f"\n✅ Loaded admin data from DB: {len(admin):,} rows | {len(admin.columns)} columns")

# ══════════════════════════════════════════════════════════════════════════
# CLEANERS   [notebook cell 8]
# ══════════════════════════════════════════════════════════════════════════
def clean_phone(phone):
    if pd.isna(phone):
        return None
    s = str(phone).strip()
    if s.lower() in ['nan','none','','null','<na>','n/a','na']:
        return None
    while s.startswith("'"):
        s = s[1:].strip()
    while s.startswith("`"):
        s = s[1:].strip()
    if s.endswith('.0'):
        s = s[:-2]
    if 'e' in s.lower() and any(c.isdigit() for c in s):
        try:
            s = str(int(float(s)))
        except Exception:
            pass
    is_international = s.startswith('+') and not s.startswith('+91')
    if is_international:
        rest = re.sub(r'[\s\-\(\)\.]', '', s[1:])
        return '+' + rest if rest else None
    s = re.sub(r'[\s\-\(\)\.\+]', '', s)
    if not s or s.lower() == 'nan':
        return None
    if s.startswith('91') and len(s) == 12 and s[2:].isdigit():
        s = s[2:]
    elif s.startswith('0') and len(s) == 11 and s.isdigit():
        s = s[1:]
    return s if s else None

def clean_email(email):
    if pd.isna(email):
        return None
    s = str(email).strip().lower()
    while s.startswith("'"):
        s = s[1:].strip()
    if s in ['nan','none','','null','<na>','n/a','na'] or '@' not in s:
        return None
    return s

# ══════════════════════════════════════════════════════════════════════════
# READ STATIC FILES   [notebook cell 9]
# ══════════════════════════════════════════════════════════════════════════
def read_csv_from_zip(zip_path):
    with zipfile.ZipFile(zip_path, 'r') as z:
        csv_filename = z.namelist()[0]
        with z.open(csv_filename) as f:
            return pd.read_csv(f, low_memory=False)

orders_1 = read_csv_from_zip(os.path.join(OUTPUT_DIR, "orders_export_1 (2).zip"))
orders_2 = read_csv_from_zip(os.path.join(OUTPUT_DIR, "orders_export_2 (2).zip"))
orders_3 = read_csv_from_zip(os.path.join(OUTPUT_DIR, "orders_export_3 (2).zip"))
orders_shopify = pd.concat([orders_1, orders_2, orders_3], ignore_index=True)

customers_shopify   = read_csv_from_zip(os.path.join(OUTPUT_DIR, "customers_export (2).zip"))
zip_shopify         = pd.read_csv(os.path.join(OUTPUT_DIR, "zip_shopify.csv"))
Indian_zip_codes    = pd.read_csv(os.path.join(OUTPUT_DIR, "Indian zip codes_modified.zip.csv"))
customer_id_shopify = pd.read_csv(os.path.join(OUTPUT_DIR, "Orders by customer email (1).csv"))

print("\n✅ Data loaded")
print(f"   Shopify orders:    {orders_shopify.shape}")
print(f"   Shopify customers: {customers_shopify.shape}")
print(f"   Admin:             {admin.shape}")
print(f"   B2B:               {B2B.shape}")


# ==========================================================================
# TRANSFORMATION PIPELINE (Sections 2-19) — verbatim from notebook
# ==========================================================================


# ---- notebook cell 10 --------------------------------------------------
# SECTION 2 — SHOPIFY ORDERS cleaning
# ============================================================================
 
# ── 2.1 Phone cleaning (uses central clean_phone)
orders_shopify['Phone'] = orders_shopify['Phone'].apply(clean_phone)
 
# ── 2.2 Source → sub_channel mapping
source_mapping = {
    '232244117505'        : 'magic_checkout',
    'web'                 : 'online_store',
    '4926501'             : 'evlop_app',
    'shopify_draft_order' : 'draft_orders',
    'iphone'              : 'shopify_iphone',
    '75716952065'         : 'kwikchat',
    '2653365'             : 'shopney_app',
}
sub_channel_mapping = {
    'magic_checkout' : 'Shopify_Website',
    'draft_orders'   : 'Shopify_Draft_Orders',
    'online_store'   : 'Shopify_Website',
    'evlop_app'      : 'Shopify_App',
    'shopify_iphone' : 'Shopify_Website',
    'kwikchat'       : 'Shopify_Website',
    'shopney_app'    : 'Shopify_App',
}
orders_shopify['source_channel'] = (
    orders_shopify['Source'].astype(str).map(source_mapping).fillna('online_store')
)
orders_shopify['sub_channel'] = orders_shopify['source_channel'].map(sub_channel_mapping)
 
# ── 2.3 Unique key (Phone is already clean now, so unique_key is consistent)
orders_shopify['unique_key'] = (
    orders_shopify['Name'].astype(str) + '_' +
    orders_shopify['Lineitem sku'].astype(str) + '_' +
    orders_shopify['Phone'].astype(str)
)
 
# ── 2.4 Date columns (UTC → IST, drop tz for Power BI)
date_cols = ['Paid at', 'Fulfilled at', 'Created at', 'Cancelled at', 'Next Payment Due At']
for col in date_cols:
    orders_shopify[col] = pd.to_datetime(orders_shopify[col], utc=True, errors='coerce')
    orders_shopify[col] = orders_shopify[col].dt.tz_convert('Asia/Kolkata').dt.tz_localize(None)
 
# ── 2.5 Numeric columns
num_cols = ['Subtotal', 'Shipping', 'Taxes', 'Total', 'Discount Amount',
            'Lineitem quantity', 'Lineitem price', 'Refunded Amount',
            'Outstanding Balance', 'Lineitem discount']
for col in num_cols:
    orders_shopify[col] = pd.to_numeric(orders_shopify[col], errors='coerce')
 
# ── 2.6 Zip cleaning
orders_shopify['Billing Zip']  = orders_shopify['Billing Zip'].astype(str).str.replace("'", '').str.strip()
orders_shopify['Shipping Zip'] = orders_shopify['Shipping Zip'].astype(str).str.replace("'", '').str.strip()
 
# ── 2.6b Clean Billing Phone & Shipping Phone (uses central clean_phone)
for phone_col in ['Billing Phone', 'Shipping Phone']:
    if phone_col in orders_shopify.columns:
        orders_shopify[phone_col] = orders_shopify[phone_col].apply(clean_phone)
 
# ── 2.7 Strip whitespace + standardize status
obj_cols = orders_shopify.select_dtypes(include='object').columns
orders_shopify[obj_cols] = orders_shopify[obj_cols].apply(lambda x: x.str.strip())
orders_shopify['Financial Status']   = orders_shopify['Financial Status'].str.lower()
orders_shopify['Fulfillment Status'] = orders_shopify['Fulfillment Status'].str.lower()
 
# ── 2.8 Drop blank rows
orders_shopify = orders_shopify.dropna(subset=['Name'])
 
# ── 2.9 Drop unnecessary columns
drop_cols = [
    'Tax 3 Name', 'Tax 3 Value', 'Tax 4 Name', 'Tax 4 Value',
    'Tax 5 Name', 'Tax 5 Value', 'Receipt Number', 'Duties', 'Device ID',
    'Location', 'Shipping Company', 'Billing Company',
    'Next Payment Due At', 'Employee', 'Tax 2 Name',
    'Tax 2 Value', 'Payment Terms Name',
    'Note Attributes', 'Notes',
    'Payment Reference', 'Payment References',
    'Lineitem compare at price',
    'Lineitem taxable', 'Lineitem requires shipping',
    'Id', 'Tax 1 Name', 'Tax 1 Value',
    'Billing Street', 'Billing Address2', 'Billing Province',
    'Shipping Street', 'Shipping Address2', 'Shipping Province',
    'Vendor', 'Currency', 'Payment ID',
    'Source', 'source_channel',
]
orders_shopify = orders_shopify.drop(columns=drop_cols, errors='ignore')
 
# ── 2.10 Add channel + reorder
orders_shopify['channel'] = '1P'
orders_shopify = orders_shopify[[
    'channel', 'sub_channel',
    'Name', 'unique_key', 'Created at',
    'Financial Status', 'Fulfillment Status', 'Paid at', 'Fulfilled at', 'Cancelled at',
    'Email', 'Phone', 'Billing Phone', 'Shipping Phone', 'Accepts Marketing',
    'Lineitem name', 'Lineitem sku', 'Lineitem quantity', 'Lineitem price',
    'Lineitem discount', 'Lineitem fulfillment status',
    'Subtotal', 'Shipping', 'Taxes', 'Total',
    'Discount Code', 'Discount Amount', 'Refunded Amount', 'Outstanding Balance',
    'Shipping Method',
    'Billing Name', 'Billing Address1', 'Billing City', 'Billing Zip',
    'Billing Province Name', 'Billing Country',
    'Shipping Name', 'Shipping Address1', 'Shipping City', 'Shipping Zip',
    'Shipping Province Name', 'Shipping Country',
    'Payment Method', 'Tags', 'Risk Level'
]]
 
print(f"✅ Shopify orders cleaned: {orders_shopify.shape}")
print(f"   Phone columns (non-null):")
print(f"     Phone={orders_shopify['Phone'].notna().sum():,}")
print(f"     Billing Phone={orders_shopify['Billing Phone'].notna().sum():,}")
print(f"     Shipping Phone={orders_shopify['Shipping Phone'].notna().sum():,}")


# ---- notebook cell 11 --------------------------------------------------
# SECTION 3 — SHOPIFY CUSTOMERS cleaning
# ============================================================================
 
customers_shopify = customers_shopify.drop(columns=[
    'Default Address Company', 'Note', 'Default Address Address2',
    'Default Address Phone', 'Default Address Province Code',
    'Birth date (customer.metafields.facts.birth_date)', 'Tax Exempt',
], errors='ignore')
 
# ── Phone cleaning (uses central clean_phone — replaces 11-line strict lambda)
customers_shopify['Phone'] = customers_shopify['Phone'].apply(clean_phone)
 
# ── Full name
customers_shopify['Full Name'] = (
    customers_shopify['First Name'].fillna('') + ' ' +
    customers_shopify['Last Name'].fillna('')
).str.strip()
 
# ── Zip cleaning
customers_shopify['Default Address Zip'] = (
    customers_shopify['Default Address Zip'].astype(str).str.replace("'", '').str.strip()
)
 
customers_shopify['channel']     = '1P'
customers_shopify['sub_channel'] = 'shopify'
customers_shopify = customers_shopify[[
    'channel', 'sub_channel', 'Customer ID',
    'First Name', 'Last Name', 'Full Name',
    'Email', 'Phone',
    'Accepts Email Marketing', 'Accepts SMS Marketing',
    'Total Orders', 'Total Spent', 'Tags',
    'Default Address Address1', 'Default Address City',
    'Default Address Zip', 'Default Address Country Code'
]]
 
print(f"\n✅ Shopify customers cleaned: {customers_shopify.shape}")
print(f"   Phone non-null: {customers_shopify['Phone'].notna().sum():,} / {len(customers_shopify):,}")
 
 
# ============================================================================
# SECTION 4 — MERGE Shopify orders + customers
# Both Phone columns now have IDENTICAL format (10-digit Indian / +cc intl),
# so the merge on='Phone' will properly match across the two tables.
# ============================================================================


# ---- notebook cell 12 --------------------------------------------------
customers_to_merge = customers_shopify[[
    'Phone', 'Customer ID', 'Full Name',
    'Accepts Email Marketing', 'Accepts SMS Marketing',
    'Total Orders', 'Total Spent', 'Tags',
    'Default Address Address1', 'Default Address City',
    'Default Address Zip', 'Default Address Country Code'
]].drop_duplicates(subset='Phone')
 
# Drop rows where Phone is null BEFORE merge — null can't join, and keeping
# nulls in the right-hand side wastes a row that will never match anyway
customers_to_merge = customers_to_merge[customers_to_merge['Phone'].notna()]
 
shopify_data = orders_shopify.merge(
    customers_to_merge, on='Phone', how='left', suffixes=('', '_customer')
)
print(f"\n✅ Shopify data merged: {shopify_data.shape}")
print(f"   Rows with customer enrichment (Customer ID present): "
      f"{shopify_data['Customer ID'].notna().sum():,}")
 


# ---- notebook cell 13 --------------------------------------------------
# ════════════════════════════════════════════════════════════════════════
# SECTION 5 — CLEAN ADMIN DATA (now loaded from DB, not Excel)
# ════════════════════════════════════════════════════════════════════════
# DB column → pipeline-standard column. Mapped by CONTENT (verified against
# how each standard column is used downstream), not by the old Excel header
# order. So store_name→Shipping Method, is_express_delivery→Tags_customer
# (the express Yes/No source), notes→Tags_customer2 (the order-note source).
admin = admin.rename(columns={
    'order_created_date'      : 'Created at',
    'order_id'                : 'Name',
    'delivery_date'           : 'Delivery Date',
    'delivery_time_slot'      : 'Delivery Time Slot',
    'customer_name'           : 'Billing Name',
    'customer_address'        : 'Shipping Address1',
    'customer_contact'        : 'Phone',
    'product_title'           : 'Lineitem name',
    'store_name'              : 'Shipping Method',
    'is_express_delivery'     : 'Tags_customer',      # express flag source → 'Is Express'
    'notes'                   : 'Tags_customer2',     # order note source
    'sales_channel'           : 'sub_channel',
    'payment_status'          : 'Financial Status',
    'payment_mode'            : 'Payment Method',
    'fulfillment_status_code' : 'Fulfillment Status',
    'net_items_sold'          : 'Lineitem quantity',
    'gross_sales'             : 'Subtotal',
    'discount_coupon_used'    : 'Discount Code',
    'discounts'               : 'Discount Amount',
    'net_sales'               : 'Total',
    'taxes'                   : 'Taxes',
    'shipping_charges'        : 'Shipping',
    'total_sales'             : 'Total with Shipping',
})

# ── Express flag (robust to Yes/No text OR boolean true/false OR 1/0 from DB)
admin['Is Express'] = (
    admin['Tags_customer'].astype(str).str.strip().str.lower()
    .map({'yes':'Yes','no':'No','true':'Yes','false':'No','t':'Yes','f':'No','1':'Yes','0':'No'})
)

# ── Phone cleaning (central clean_phone)
admin['Phone'] = admin['Phone'].apply(clean_phone)

# ── Date parsing
admin['Created at']    = pd.to_datetime(admin['Created at'], errors='coerce')
admin['Delivery Date'] = pd.to_datetime(admin['Delivery Date'], errors='coerce')

# ── String cleanup (astype(str) FIRST — DB may return status as int codes)
admin['Financial Status']   = admin['Financial Status'].astype(str).str.strip().str.lower()
admin['Fulfillment Status'] = admin['Fulfillment Status'].astype(str).str.strip().str.lower()
admin['Payment Method']     = admin['Payment Method'].astype(str).str.strip().str.lower()
admin['sub_channel']        = admin['sub_channel'].astype(str).str.strip().str.lower()
admin['Billing Name']       = admin['Billing Name'].str.strip().str.title()
admin['Lineitem name']      = admin['Lineitem name'].str.strip()
admin['Shipping Method']    = admin['Shipping Method'].str.strip()
admin['Tags_customer']      = admin['Tags_customer'].astype(str).str.strip()
admin['Tags_customer2']     = admin['Tags_customer2'].astype(str).str.strip()

# ── Combine Tags_customer (express) + Tags_customer2 (note) → single Tags_customer
admin['Tags_customer'] = admin['Tags_customer'].fillna('') + ' | ' + admin['Tags_customer2'].fillna('')
admin['Tags_customer'] = admin['Tags_customer'].str.strip(' |').replace('', np.nan)
admin['Tags_customer'] = admin['Tags_customer'].apply(
    lambda x: None if pd.isna(x) or str(x).strip() in [
        'No | -', 'Yes | -', 'No |', 'Yes |', '-', 'No', 'Yes', '|', '',
        'nan | nan', 'nan', 'None | None', 'true', 'false'
    ] else str(x).strip()
)
admin = admin.drop(columns=['Tags_customer2'])

# ── Standard sub_channel mapping
admin_subchannel_mapping = {
    'app'         : 'Admin_App',
    'website'     : 'Admin_Website',
    'draftorder'  : 'Admin_Draft_Orders',
}
admin['sub_channel'] = admin['sub_channel'].map(admin_subchannel_mapping).fillna('Admin_Other')

# ── Blinkit + B2B detection
b2b_mask = admin['Billing Name'].str.contains('blinkit|b2b', case=False, na=False, regex=True)
admin.loc[b2b_mask, 'sub_channel'] = 'B2B_DraftOrder'
print(f"   🏢 B2B_DraftOrder detected (blinkit + b2b combined): {b2b_mask.sum()}")

# ── Numeric columns
num_cols = ['Lineitem quantity', 'Subtotal', 'Discount Amount',
            'Total', 'Taxes', 'Shipping', 'Total with Shipping']
for col in num_cols:
    admin[col] = pd.to_numeric(admin[col], errors='coerce')

admin['Zip']        = admin['Shipping Address1'].astype(str).str.extract(r'(\d{6})')
admin['channel']    = '1P'
admin['unique_key'] = (
    admin['Name'].astype(str) + '_' +
    admin['Lineitem name'].astype(str) + '_' +
    admin['Phone'].astype(str)
)

admin = admin[[
    'channel', 'sub_channel', 'Name', 'unique_key',
    'Created at', 'Delivery Date', 'Delivery Time Slot',
    'Financial Status', 'Fulfillment Status',
    'Billing Name', 'Phone',
    'Lineitem name', 'Lineitem quantity',
    'Subtotal', 'Discount Code', 'Discount Amount',
    'Taxes', 'Shipping', 'Total', 'Total with Shipping',
    'Shipping Method', 'Shipping Address1', 'Zip',
    'Payment Method', 'Tags_customer', 'Is Express',
]]

print(f"\n✅ Admin cleaned: {admin.shape}")
print(f"   Phone non-null: {admin['Phone'].notna().sum():,} / {len(admin):,}")
print(f"   sub_channel distribution:\n{admin['sub_channel'].value_counts().to_string()}")
print(f"   Is Express counts: {admin['Is Express'].value_counts().to_dict()}")
print(f"\n   ⚠️  Financial Status values now: {admin['Financial Status'].value_counts().head(8).to_dict()}")
print(f"   ⚠️  Fulfillment Status values now: {admin['Fulfillment Status'].value_counts().head(8).to_dict()}")


# ---- notebook cell 14 --------------------------------------------------
product_sql = """
SELECT DISTINCT 
    TRIM(title) AS lineitem_name,
    sku AS lineitem_sku
FROM products
"""

print("Loading product master... (5-15 sec)")
product_master = pd.read_sql(product_sql, engine)

# Clean both sides for reliable matching
product_master['lineitem_name'] = product_master['lineitem_name'].astype(str).str.strip()
product_master['lineitem_sku'] = product_master['lineitem_sku'].astype(str).str.strip()

# Drop duplicates by name (keep first sku if duplicates exist)
product_master = product_master.drop_duplicates(subset=['lineitem_name'], keep='first')

print(f"✅ Loaded product_master: {len(product_master):,} unique products")
print(f"\nSample:")
print(product_master.head(5).to_string(index=False))
print(f"\nNULL check: lineitem_name={product_master['lineitem_name'].isna().sum()}, lineitem_sku={product_master['lineitem_sku'].isna().sum()}")


# ---- notebook cell 15 --------------------------------------------------
# ──────────────────────────────────────────────────────────────────
# Add Lineitem sku to Admin DataFrame
# Match logic: admin['Lineitem name'] (trimmed) → product_master[lineitem_sku]
# ──────────────────────────────────────────────────────────────────

print("\n🔗 Mapping Lineitem sku into Admin DataFrame...")

# Step 1: Create trimmed key in admin for matching (don't overwrite original)
admin['_lineitem_name_trimmed'] = admin['Lineitem name'].astype(str).str.strip()

# Step 2: Build lookup dict (faster than merge for single column lookup)
name_to_sku = dict(zip(product_master['lineitem_name'], product_master['lineitem_sku']))

# Step 3: Map sku
admin['Lineitem sku'] = admin['_lineitem_name_trimmed'].map(name_to_sku)

# Step 4: Drop the temp column
admin = admin.drop(columns=['_lineitem_name_trimmed'])

# Step 5: Coverage report
total_rows = len(admin)
mapped_rows = admin['Lineitem sku'].notna().sum()
unmapped_rows = total_rows - mapped_rows

print(f"✅ Lineitem sku column added to Admin")
print(f"   Total Admin rows:    {total_rows:,}")
print(f"   Successfully mapped: {mapped_rows:,} ({mapped_rows/total_rows*100:.1f}%)")
print(f"   Unmapped (NaN):      {unmapped_rows:,} ({unmapped_rows/total_rows*100:.1f}%)")

# Show unmapped product names for investigation (if any)
if unmapped_rows > 0:
    unmapped_names = (
        admin[admin['Lineitem sku'].isna()]['Lineitem name']
        .value_counts()
        .head(10)
    )
    print(f"\n⚠️  Top 10 unmapped product names (check spelling/data):")
    print(unmapped_names.to_string())


# ---- notebook cell 16 --------------------------------------------------
B2B = B2B.rename(columns={
    'Invoice Date'                    : 'Created at',
    'Invoice Number'                  : 'Name',
    'Customer Name'                   : 'Billing Name',
    'Place of Supply(With State Code)': 'Shipping Province Name',
    'PurchaseOrder'                   : 'Tags_customer',
    'Item Name'                       : 'Lineitem name',
    'Quantity'                        : 'Lineitem quantity',
    'Item Total'                      : 'Lineitem price',       # line-level: Qty × Item Price
    'Usage unit'                      : 'Lineitem sku',
    'Item Price'                      : 'Lineitem unit price',  # per-unit price
    'Shipping Attention'              : 'Shipping Name',
    'Shipping State'                  : 'Shipping Province',
    'Invoice Status'                  : 'Fulfillment Status',   # Void/Open/Closed/Overdue
})
B2B['channel']     = 'B2B'
B2B['sub_channel'] = 'Zoho_Invoice'
B2B['unique_key']  = B2B['Name'].astype(str) + '_' + B2B['Lineitem name'].astype(str)
 
# Text cleaning
B2B['Billing Name']      = B2B['Billing Name'].astype(str).str.strip().str.title()
B2B['Shipping Name']     = B2B['Shipping Name'].astype(str).str.strip().str.title()
B2B['Shipping Province'] = B2B['Shipping Province'].astype(str).str.strip().str.title()
B2B['Lineitem name']     = B2B['Lineitem name'].astype(str).str.strip()
B2B['Lineitem sku']      = B2B['Lineitem sku'].astype(str).str.strip().str.lower()
B2B['Shipping Province Name'] = (
    B2B['Shipping Province Name'].astype(str).str.split('-').str[1].str.strip()
)
B2B['Fulfillment Status'] = B2B['Fulfillment Status'].astype(str).str.strip().str.title()
B2B['Created at'] = pd.to_datetime(B2B['Created at'], errors='coerce')
 
# Numeric coercion
for col in ['Lineitem quantity', 'Lineitem price', 'Lineitem unit price']:
    if col in B2B.columns:
        B2B[col] = pd.to_numeric(B2B[col], errors='coerce')
 
# SubTotal / Total: invoice-level, only on first line of each invoice
B2B = B2B.sort_values(['Name', 'Lineitem name']).reset_index(drop=True)
invoice_totals = B2B.groupby('Name')['Lineitem price'].transform('sum')
is_first_line = ~B2B['Name'].duplicated(keep='first')
B2B['Subtotal'] = pd.NA
B2B['Total']    = pd.NA
B2B.loc[is_first_line, 'Subtotal'] = invoice_totals[is_first_line]
B2B.loc[is_first_line, 'Total']    = invoice_totals[is_first_line]
 
B2B = B2B[[
    'channel', 'sub_channel', 'Name', 'unique_key',
    'Created at', 'Billing Name',
    'Lineitem name', 'Lineitem sku', 'Lineitem quantity',
    'Lineitem price', 'Lineitem unit price',
    'Subtotal', 'Total',
    'Shipping Name', 'Shipping Province', 'Shipping Province Name',
    'Tags_customer',
    'Fulfillment Status',
]]
print(f"\n✅ B2B (Zoho) cleaned: {len(B2B)} line items, {B2B['Name'].nunique()} invoices")
print(f"   💰 Total revenue (sum of line items): ₹{B2B['Lineitem price'].sum():,.0f}")
print(f"   💰 Total revenue (sum of invoice Totals): ₹{B2B['Total'].sum():,.0f}")
print(f"   📋 Invoice Status breakdown:")
print(B2B.drop_duplicates('Name')['Fulfillment Status'].value_counts())
 


# ---- notebook cell 17 --------------------------------------------------
# ──────────────────────────────────────────────────────────────────
# v7 NEW: Pre-filter B2B_DraftOrder BEFORE concat (exclude from pipeline)
# These rows are duplicates of Zoho data — keep them out of analytics
# ──────────────────────────────────────────────────────────────────
b2b_draft_to_exclude = admin[admin['sub_channel'] == 'B2B_DraftOrder'].copy()
admin_clean = admin[admin['sub_channel'] != 'B2B_DraftOrder'].copy()

print(f"📌 B2B Draft Orders → EXCLUDED from pipeline:")
print(f"   Rows excluded:                 {len(b2b_draft_to_exclude):>6,}")
print(f"   Admin rows after exclusion:    {len(admin_clean):>6,}")

# Concat with cleaned admin (B2B = Zoho only now)
master_orders = pd.concat([shopify_data, admin_clean, B2B], ignore_index=True)
print(f"\n✅ master_orders combined: {master_orders.shape}")
print(f"\n   Channel split:")
print(master_orders['channel'].value_counts().to_string())
print(f"\n   Sub-channel split:")
print(master_orders['sub_channel'].value_counts().to_string())

# Quick sanity check on phone coverage AT MASTER LEVEL (before customer_key)
print(f"\n📞 Pre-customer_key phone coverage in master_orders:")
for col in ['Phone', 'Billing Phone', 'Shipping Phone', 'Email']:
    if col in master_orders.columns:
        print(f"   {col}: {master_orders[col].notna().sum():,} / {len(master_orders):,}")



# ---- notebook cell 18 --------------------------------------------------
# ──────────────────────────────────────────────────────────────────
# v7 NEW: Bifurcate B2B (Zoho) by customer name
#   - "CPC" in name      → B2C / Blinkit
#   - "Zomato" in name   → B2B / B2B_Hyperpure
#   - Everything else    → B2B / B2B_Institutional
# B2B_DraftOrder already excluded in earlier cell
# ──────────────────────────────────────────────────────────────────
print("🔀 Bifurcating B2B channel by customer name...\n")

mask_b2b_zoho = (master_orders['channel'] == 'B2B') & (master_orders['sub_channel'] == 'Zoho_Invoice')
print(f"   Total B2B (Zoho) rows: {mask_b2b_zoho.sum():,}")

# Build bifurcation masks (case-insensitive contains)
mask_cpc = mask_b2b_zoho & master_orders['Billing Name'].astype(str).str.contains('CPC', case=False, na=False)
mask_zomato = mask_b2b_zoho & master_orders['Billing Name'].astype(str).str.contains('Zomato', case=False, na=False) & ~mask_cpc
mask_institutional = mask_b2b_zoho & ~mask_cpc & ~mask_zomato

# Apply channel + sub_channel reassignments
master_orders.loc[mask_cpc, 'channel'] = 'B2C'
master_orders.loc[mask_cpc, 'sub_channel'] = 'Blinkit'

master_orders.loc[mask_zomato, 'sub_channel'] = 'B2B_Hyperpure'   # channel stays 'B2B'
master_orders.loc[mask_institutional, 'sub_channel'] = 'B2B_Institutional'  # channel stays 'B2B'

# Report
print(f"\n   ✅ Bifurcation complete:")
print(f"      B2C / Blinkit:             {mask_cpc.sum():>6,} rows  (CPC in name)")
print(f"      B2B / B2B_Hyperpure:       {mask_zomato.sum():>6,} rows  (Zomato in name, non-CPC)")
print(f"      B2B / B2B_Institutional:   {mask_institutional.sum():>6,} rows  (all others)")

print(f"\n📊 Final channel distribution:")
print(master_orders['channel'].value_counts().to_string())

print(f"\n📋 Final sub_channel distribution:")
print(master_orders['sub_channel'].value_counts().to_string())

# Sanity check — sample customer names per new bucket
print(f"\n🔍 Sample customers per new bucket:")
for ch_name, m in [('B2C/Blinkit', mask_cpc), ('B2B/Hyperpure', mask_zomato), ('B2B/Institutional', mask_institutional)]:
    sample = master_orders.loc[m, 'Billing Name'].drop_duplicates().head(3).tolist()
    print(f"   {ch_name}: {sample}")



# ---- notebook cell 19 --------------------------------------------------
# SECTION 8 — CUSTOMER KEY creation
# This is your existing Section 7-9 logic — UNCHANGED.
# Now it receives clean Phone/Billing Phone/Shipping Phone columns from
# all upstream sections, so the coalesce + propagation works correctly.
# ============================================================================
 
phone_cols = [c for c in ['Phone', 'Billing Phone', 'Shipping Phone']
              if c in master_orders.columns]
email_col  = 'Email' if 'Email' in master_orders.columns else None
 
print(f"\n📞 Coalesce sources: phones={phone_cols}, email={email_col}")
 
 
def coalesce_customer_key(row):
    for col in phone_cols:
        v = row.get(col)
        if pd.notna(v) and str(v).strip() not in ['', 'nan', 'None']:
            return str(v).strip()
    if email_col:
        v = clean_email(row.get(email_col))
        if v:
            return v
    return None
 
 
master_orders['customer_key'] = master_orders.apply(coalesce_customer_key, axis=1)
 
master_orders['customer_key'] = master_orders['customer_key'].replace(
    ['nan', 'None', '', 'null', '<NA>'], np.nan
)
 
# Propagate within same order (groupby Name)
order_keys = (
    master_orders[master_orders['customer_key'].notna()]
    .groupby('Name')['customer_key']
    .first()
)
before_blank = master_orders['customer_key'].isna().sum()
master_orders['customer_key'] = master_orders['customer_key'].fillna(
    master_orders['Name'].map(order_keys)
)
after_blank = master_orders['customer_key'].isna().sum()
 
 
def validate_key(key):
    if pd.isna(key):
        return 'no_phone'
    s = str(key).strip()
    if '@' in s:
        return 'email_identity'
    if len(s) == 10 and s.isdigit() and s[0] in '6789':
        return 'valid_indian'
    if s.startswith('+') and not s.startswith('+91'):
        return 'international'
    if len(s) == 10 and s.isdigit():
        return 'valid_indian'
    return 'invalid'
 
 
master_orders['phone_validity'] = master_orders['customer_key'].apply(validate_key)
 
print(f"\n✅ Customer key creation complete")
print(f"   Total rows: {len(master_orders):,}")
print(f"   With customer_key: {master_orders['customer_key'].notna().sum():,}")
print(f"   Without (still blank): {master_orders['customer_key'].isna().sum():,}")
print(f"   Recovered via same-order propagation: {before_blank - after_blank:,}")
print(f"\n   Validity distribution:")
print(master_orders['phone_validity'].value_counts().to_string())
 
# Sample of invalid keys for ops review
invalid_sample = (
    master_orders[master_orders['phone_validity'] == 'invalid']
    [['Name', 'customer_key'] + phone_cols + ([email_col] if email_col else [])]
    .drop_duplicates('customer_key')
    .head(10)
)
if len(invalid_sample) > 0:
    print(f"\n🔍 Sample of 'invalid' keys (for manual review):")
    print(invalid_sample.to_string(index=False))
 
# Duplicacy key
master_orders['duplicacy_key'] = (
    master_orders['sub_channel'].astype(str) + '_' +
    master_orders['customer_key'].astype(str)
)
master_orders.loc[master_orders['customer_key'].isna(), 'duplicacy_key'] = np.nan



# ---- notebook cell 20 --------------------------------------------------
# ============================================================
#  PATCH: manual customer_key overrides for specific order_ids
#  Paste AFTER customer_key has been created in master_orders.
#  Edits master_orders in place — final df stays master_orders.
# ============================================================

ORDER_COL   = "Name"           # order id column in master_orders (with #)
CUSTKEY_COL = "customer_key"   # the customer key column

# --- the manual map: order_id -> correct customer_key ---
overrides = {
    "#5617": "9999844428", "#5408": "9999844428", "#5296": "9999844428",
    "#4314": "9810060389", "#3869": "9810060389",
    "#3503": "7303933525", "#3433": "7303933525",
    "#2968": "9102005665",
    "#DN18579": "61407517076", "#16290": "61407517076", "#15102": "61407517076",
    "#14533": "9142178472",
    "#2099": "9811043225", "#5887": "9811043225", "#6314": "9811043225",
    "#7153": "9811043225", "#11939": "9811043225", "#15548": "9811043225",
    "#DN18791": "9811043225", "#DN19751": "9811043225",
    "#7088": "8825614020",
    "#7121": "8920574966",
    "#1017": "9512693877", "#1016": "9512693877", "#1015": "9512693877",
    "#7089": "9599130991", "#7070": "9625623775", "#7139": "9650907202",
    "#7099": "9650991045", "#7084": "9663701261", "#7081": "9717702742",
    "#7150": "9810004168", "#7096": "9810595092", "#7110": "9810607343",
    "#7119": "9810685920", "#7102": "9810702929", "#7029": "9811041400",
    "#7077": "9811097794", "#7125": "9873174448", "#7064": "9873307778",
    "#1026": "9898869554", "#1022": "9898869554",
    "#7034": "9910722202", "#7082": "9928507196", "#7120": "9933285039",
    "#7148": "9958861150", "#7124": "9999593010", "#7489": "9313193973",
    "#6270": "9811055980", "#5806": "1407517076",
    "#12593": "5167178899", "#8426": "5167178899",
    "#12917": "7303241692", "#5947": "9136290878", "#5753": "9315647063",
    "#11192": "9492540515", "#10747": "9492540515",
    "#7480": "9899281442", "#2590": "9811059163", "#3665": "8375979819",
}

# normalise order id on master side, map overrides
_key  = master_orders[ORDER_COL].astype(str).str.strip()
_new  = _key.map(overrides)
applied = _new.notna()

# capture before-state for reporting
before_blank = master_orders[CUSTKEY_COL].isna() | (
    master_orders[CUSTKEY_COL].astype(str).str.strip() == ""
)

# apply overrides
master_orders.loc[applied, CUSTKEY_COL] = _new[applied].values

# --- reporting (pipeline style) ---
matched_ids   = set(_key[applied])
missing_ids   = sorted(set(overrides) - matched_ids)
filled_blanks = (applied & before_blank).sum()

print("✅ Manual customer_key override complete")
print(f"   Override list size         : {len(overrides)} order ids")
print(f"   Order ids matched in data  : {applied.sum()}")
print(f"   Of those, were blank before: {filled_blanks}")
print(f"   Of those, overwrote a value: {applied.sum() - filled_blanks}")
print(f"   Order ids NOT found ({len(missing_ids)}): {missing_ids}")
print()
print("   customer_key coverage now:")
_now_blank = master_orders[CUSTKEY_COL].isna() | (
    master_orders[CUSTKEY_COL].astype(str).str.strip() == ""
)
print(f"     Total rows         : {len(master_orders):,}")
print(f"     With customer_key  : {(~_now_blank).sum():,}")
print(f"     Without (blank)    : {_now_blank.sum():,}")


# ---- notebook cell 21 --------------------------------------------------
# ============================================================
#  PHONE VALIDITY CLASSIFIER  (fresh, standard Indian rules)
#  Paste AFTER customer_key + manual overrides are done.
#  Adds/overwrites 'phone_validity'. Edits master_orders in place.
# ============================================================

CUSTKEY_COL  = "customer_key"
VALIDITY_COL = "phone_validity"

def classify(raw):
    # blank / null
    if pd.isna(raw):
        return "no_phone"
    s = str(raw).strip()
    if s == "" or s.lower() in ("nan", "none"):
        return "no_phone"

    # email used as the identity key
    if "@" in s:
        return "email_identity"

    # strip formatting: spaces, +, -, (), dots
    digits = re.sub(r"[\s\+\-\(\)\.]", "", s)

    # if after stripping it's not all digits -> invalid (junk chars)
    if not digits.isdigit():
        return "invalid"

    # normalise common Indian prefixes: leading 0, 91, 0091
    if digits.startswith("0091"):
        digits = digits[4:]
    elif digits.startswith("91") and len(digits) == 12:
        digits = digits[2:]
    elif digits.startswith("0") and len(digits) == 11:
        digits = digits[1:]

    # clean 10-digit Indian mobile (starts 6/7/8/9)
    if len(digits) == 10 and digits[0] in "6789":
        return "valid_indian"

    # plausible foreign number (all digits, longer than Indian mobile)
    if 11 <= len(digits) <= 15:
        return "international"

    # everything else: wrong length, bad start digit, etc.
    return "invalid"

master_orders[VALIDITY_COL] = master_orders[CUSTKEY_COL].apply(classify)

# --- reporting (pipeline style) ---
print("✅ Phone validity classification complete")
print(f"   Total rows: {len(master_orders):,}")
print()
print("   Validity distribution:")
counts = master_orders[VALIDITY_COL].value_counts()
for cat, n in counts.items():
    print(f"     {cat:<15} {n:>6,}")


# ---- notebook cell 22 --------------------------------------------------
print("🔄 Propagating order-level fields...")

order_level_fields = [
    'sub_channel', 'channel', 'order_status',
    'Financial Status', 'Fulfillment Status',
    'Paid at', 'Fulfilled at', 'Cancelled at',
    'customer_key', 'phone_validity',
    'Email', 'Phone',
    'Billing Name',
    'Shipping Name',
    'city', 'state', 'zip',
    'Payment Method', 'Discount Code', 'Shipping Method',
    'Full Name', 'Customer ID', 'Tags_customer',
]
order_level_fields = [c for c in order_level_fields if c in master_orders.columns]

STATUS_PRIORITY = {
    'Completed': 1, 'Pending': 2, 'Refunded': 3,
    'Cancelled': 4, 'Test_Order': 5, 'Test Orders': 5,
    'Voided': 6, 'Other': 99
}
SUBCH_PRIORITY = {
    'Shopify_Website':        1,
    'Shopify_App':            2,
    'Shopify_Draft_Orders':   3,
    'Admin_App':              4,
    'Admin_Website':          5,
    'Admin_Draft_Orders':     6,
    'Blinkit_DraftOrder':     7,
}

changes_count = {}
for field in order_level_fields:
    if field == 'order_status':
        master_orders['_temp_rank'] = master_orders[field].map(STATUS_PRIORITY).fillna(100)
        best_values = master_orders.sort_values('_temp_rank').groupby('Name')[field].first()
        master_orders = master_orders.drop(columns=['_temp_rank'])
    elif field == 'sub_channel':
        master_orders['_temp_rank'] = master_orders[field].map(SUBCH_PRIORITY).fillna(50)
        best_values = master_orders.sort_values('_temp_rank').groupby('Name')[field].first()
        master_orders = master_orders.drop(columns=['_temp_rank'])
    else:
        best_values = (
            master_orders[master_orders[field].notna()]
            .groupby('Name')[field].first()
        )
    
    before_vals = master_orders.set_index('Name')[field]
    after_vals = before_vals.index.map(best_values)
    diff_mask = (before_vals.values != after_vals) & pd.Series(after_vals).notna().values
    changes = int(diff_mask.sum())
    
    if changes > 0:
        changes_count[field] = changes
        master_orders[field] = master_orders['Name'].map(best_values).fillna(master_orders[field])

print(f"\n✅ Fields propagated:")
for field, count in sorted(changes_count.items(), key=lambda x: -x[1]):
    print(f"   {field:25s}: {count:>5,} rows updated")


# ---- notebook cell 23 --------------------------------------------------
# ── v7: Channel masks (UPDATED for new bifurcation)
mask_1p_shopify = (master_orders['channel'] == '1P') & (
    master_orders['sub_channel'].isin([
        'Shopify_Website', 'Shopify_App', 'Shopify_Draft_Orders'
    ])
)
mask_1p_admin = (master_orders['channel'] == '1P') & (
    master_orders['sub_channel'].astype(str).str.startswith('Admin')
)
mask_1p = mask_1p_shopify | mask_1p_admin

# v7 CHANGE: B2B now includes Hyperpure + Institutional (Zoho schema); B2C = Blinkit (Zoho schema)
# All three use the same Zoho line-item structure
mask_b2b_zoho = (
    ((master_orders['channel'] == 'B2B') & master_orders['sub_channel'].isin(['B2B_Hyperpure', 'B2B_Institutional']))
    | ((master_orders['channel'] == 'B2C') & (master_orders['sub_channel'] == 'Blinkit'))
)

print(f"📋 Processing:")
print(f"   Shopify (1P):       {mask_1p_shopify.sum():,} rows  — proportional discount allocation")
print(f"   Admin (1P):         {mask_1p_admin.sum():,} rows  — direct source Net sales")
print(f"   B2B/B2C (Zoho):     {mask_b2b_zoho.sum():,} rows  — line-level Lineitem price")

# ── 10.1: SHOPIFY only — recover missing Lineitem price from Subtotal/qty
needs_recovery = (
    mask_1p_shopify &
    ((master_orders['Lineitem price'].isna()) | (master_orders['Lineitem price'] == 0)) &
    (master_orders['Subtotal'].notna()) & (master_orders['Subtotal'] > 0) &
    (master_orders['Lineitem quantity'].notna()) & (master_orders['Lineitem quantity'] > 0)
)
n_recovered = needs_recovery.sum()
if n_recovered > 0:
    master_orders.loc[needs_recovery, 'Lineitem price'] = (
        master_orders.loc[needs_recovery, 'Subtotal'] /
        master_orders.loc[needs_recovery, 'Lineitem quantity']
    )
    print(f"   ✅ Recovered Lineitem price (Shopify): {n_recovered:,} rows")

# ── 10.2: Initialize columns
master_orders['Lineitem_Revenue']  = 0.0
master_orders['Lineitem_Discount'] = 0.0
master_orders['Gross_Revenue']     = 0.0   # v7 NEW: was 'Net_Revenue'

# ── 10.3: SHOPIFY — qty × price, proportional discount allocation
master_orders.loc[mask_1p_shopify, 'Lineitem_Revenue'] = (
    master_orders.loc[mask_1p_shopify, 'Lineitem quantity'].fillna(0) *
    master_orders.loc[mask_1p_shopify, 'Lineitem price'].fillna(0)
)

order_discount_shopify = (
    master_orders[mask_1p_shopify].groupby('Name')['Discount Amount']
    .max().fillna(0)
)
order_gross_shopify = master_orders[mask_1p_shopify].groupby('Name')['Lineitem_Revenue'].sum()

master_orders['_order_discount'] = master_orders['Name'].map(order_discount_shopify)
master_orders['_order_gross']    = master_orders['Name'].map(order_gross_shopify)

master_orders.loc[mask_1p_shopify, 'Lineitem_Discount'] = np.where(
    master_orders.loc[mask_1p_shopify, '_order_gross'] > 0,
    (master_orders.loc[mask_1p_shopify, 'Lineitem_Revenue'] /
     master_orders.loc[mask_1p_shopify, '_order_gross']) *
    master_orders.loc[mask_1p_shopify, '_order_discount'],
    0
)

master_orders.loc[mask_1p_shopify, 'Gross_Revenue'] = (
    master_orders.loc[mask_1p_shopify, 'Lineitem_Revenue'] -
    master_orders.loc[mask_1p_shopify, 'Lineitem_Discount']
).clip(lower=0)

# ── 10.4: ADMIN (1P) — use source's "Net sales" (already in 'Total')
master_orders.loc[mask_1p_admin, 'Lineitem_Revenue']  = (
    master_orders.loc[mask_1p_admin, 'Subtotal'].fillna(0)
)
master_orders.loc[mask_1p_admin, 'Lineitem_Discount'] = (
    master_orders.loc[mask_1p_admin, 'Discount Amount'].fillna(0)
)
master_orders.loc[mask_1p_admin, 'Gross_Revenue'] = (
    master_orders.loc[mask_1p_admin, 'Total'].fillna(0)
).clip(lower=0)

# ── 10.5: B2B/B2C ZOHO (Hyperpure, Institutional, Blinkit) — line-level
master_orders.loc[mask_b2b_zoho, 'Lineitem_Revenue']  = master_orders.loc[mask_b2b_zoho, 'Lineitem price'].fillna(0)
master_orders.loc[mask_b2b_zoho, 'Lineitem_Discount'] = 0
master_orders.loc[mask_b2b_zoho, 'Gross_Revenue']     = master_orders.loc[mask_b2b_zoho, 'Lineitem price'].fillna(0)

# ── 10.6: Reconcile order-level Total (Shopify only)
order_total_shopify = (
    master_orders[mask_1p_shopify].groupby('Name')['Gross_Revenue'].sum().round(2)
)
master_orders.loc[mask_1p_shopify, 'Total'] = (
    master_orders.loc[mask_1p_shopify, 'Name'].map(order_total_shopify)
)

# ── 10.7: Discount ratio
master_orders['Discount_Ratio'] = np.where(
    master_orders['Lineitem_Revenue'] > 0,
    (master_orders['Lineitem_Discount'] / master_orders['Lineitem_Revenue']) * 100,
    0
).clip(0, 100)

# ── 10.8: Data quality flag
def get_flag(row):
    qty, price = row['Lineitem quantity'], row['Lineitem price']
    rev, gross, disc = row['Lineitem_Revenue'], row['Gross_Revenue'], row['Lineitem_Discount']
    if pd.isna(qty) or qty == 0:
        return 'missing_quantity'
    if (pd.isna(price) or price == 0) and rev == 0:
        return 'missing_price_no_subtotal'
    if rev == 0:
        return 'zero_revenue'
    if gross == 0 and disc > 0:
        return 'fully_discounted'
    return 'ok'

master_orders['_data_quality_flag'] = master_orders.apply(get_flag, axis=1)

# Cleanup + rounding
master_orders = master_orders.drop(columns=['_order_discount', '_order_gross'])
for col in ['Lineitem price', 'Lineitem_Revenue', 'Lineitem_Discount',
            'Gross_Revenue', 'Discount_Ratio', 'Total']:
    if col in master_orders.columns:
        master_orders[col] = pd.to_numeric(master_orders[col], errors='coerce').round(2)

# ── Validation
print(f"\n🔍 Data quality:")
for flag, n in master_orders['_data_quality_flag'].value_counts().items():
    pct = n / len(master_orders) * 100
    icon = '✅' if flag == 'ok' else '🟡' if flag in ('fully_discounted', 'zero_revenue') else '🔴'
    print(f"   {icon} {flag:30s}: {n:>6,} ({pct:>5.1f}%)")

# Shopify reconciliation only
order_check = master_orders[mask_1p_shopify].groupby('Name').agg(
    gross_sum=('Gross_Revenue', 'sum'),
    total=('Total', 'max'),
)
mismatches = ((order_check['gross_sum'] - order_check['total']).abs() > 1).sum()
print(f"\n🎯 Shopify Reconciliation: {mismatches} orders mismatched (target: 0)")

print(f"\n✅ TOTAL GROSS REVENUE (before tax-net): ₹{master_orders['Gross_Revenue'].sum():,.2f}")
print(f"   Shopify (1P):       ₹{master_orders.loc[mask_1p_shopify, 'Gross_Revenue'].sum():,.2f}")
print(f"   Admin (1P):         ₹{master_orders.loc[mask_1p_admin, 'Gross_Revenue'].sum():,.2f}")
print(f"   B2B/B2C (Zoho):     ₹{master_orders.loc[mask_b2b_zoho, 'Gross_Revenue'].sum():,.2f}")

# ══════════════════════════════════════════════════════════════════════════
# 10.X — SHIPPING ADDITION (UNCHANGED logic, just uses Gross_Revenue now)
# ══════════════════════════════════════════════════════════════════════════
print(f"\n📦 Adding shipping to Gross_Revenue...")

master_orders['Lineitem_Shipping'] = 0.0

if 'Shipping' in master_orders.columns:
    shopify_order_shipping = (
        master_orders[mask_1p_shopify]
        .groupby('Name')['Shipping']
        .max()
        .fillna(0)
    )
    shopify_order_gross = (
        master_orders[mask_1p_shopify]
        .groupby('Name')['Lineitem_Revenue']
        .sum()
    )

    master_orders['_ship_order_total'] = master_orders['Name'].map(shopify_order_shipping)
    master_orders['_ship_order_gross'] = master_orders['Name'].map(shopify_order_gross)

    master_orders.loc[mask_1p_shopify, 'Lineitem_Shipping'] = np.where(
        master_orders.loc[mask_1p_shopify, '_ship_order_gross'] > 0,
        (master_orders.loc[mask_1p_shopify, 'Lineitem_Revenue'] /
         master_orders.loc[mask_1p_shopify, '_ship_order_gross']) *
        master_orders.loc[mask_1p_shopify, '_ship_order_total'],
        0
    )

    master_orders.loc[mask_1p_admin, 'Lineitem_Shipping'] = (
        master_orders.loc[mask_1p_admin, 'Shipping'].fillna(0)
    )

    master_orders = master_orders.drop(columns=['_ship_order_total', '_ship_order_gross'])
else:
    print("   ⚠️  'Shipping' column not found — skipping shipping addition")

# Add shipping into Gross_Revenue
master_orders['Gross_Revenue'] = (
    master_orders['Gross_Revenue'].fillna(0) +
    master_orders['Lineitem_Shipping'].fillna(0)
).round(2)

total_ship = master_orders['Lineitem_Shipping'].sum()
print(f"   ✅ Total shipping added: ₹{total_ship:,.2f}")
print(f"      Shopify shipping:    ₹{master_orders.loc[mask_1p_shopify, 'Lineitem_Shipping'].sum():,.2f}")
print(f"      Admin shipping:      ₹{master_orders.loc[mask_1p_admin, 'Lineitem_Shipping'].sum():,.2f}")

print(f"\n✅ GROSS REVENUE (after shipping, before tax adjustment): ₹{master_orders['Gross_Revenue'].sum():,.2f}")

# ══════════════════════════════════════════════════════════════════════════
# v7 NEW: TAX CALCULATION + NET_REVENUE (Gross - Tax)
# ══════════════════════════════════════════════════════════════════════════
print(f"\n" + "="*70)
print(f"v7 NEW: Tax calculation + Net_Revenue")
print(f"="*70)

# Initialize Tax_Amount column
master_orders['Tax_Amount'] = 0.0

# ── Shopify: 'Taxes' column is at order level (only on first line)
#    Distribute proportionally across line items by Lineitem_Revenue
if 'Taxes' in master_orders.columns:
    shopify_order_tax = (
        master_orders[mask_1p_shopify]
        .groupby('Name')['Taxes']
        .max()
        .fillna(0)
    )
    shopify_order_gross_rev = (
        master_orders[mask_1p_shopify]
        .groupby('Name')['Lineitem_Revenue']
        .sum()
    )
    master_orders['_tax_order_total'] = master_orders['Name'].map(shopify_order_tax)
    master_orders['_tax_order_gross'] = master_orders['Name'].map(shopify_order_gross_rev)
    
    master_orders.loc[mask_1p_shopify, 'Tax_Amount'] = np.where(
        master_orders.loc[mask_1p_shopify, '_tax_order_gross'] > 0,
        (master_orders.loc[mask_1p_shopify, 'Lineitem_Revenue'] /
         master_orders.loc[mask_1p_shopify, '_tax_order_gross']) *
        master_orders.loc[mask_1p_shopify, '_tax_order_total'],
        0
    )
    master_orders = master_orders.drop(columns=['_tax_order_total', '_tax_order_gross'])
    print(f"   ✅ Shopify tax distributed line-item-wise")

# ── Admin: 'Taxes' column is already line-item level — use directly
master_orders.loc[mask_1p_admin, 'Tax_Amount'] = (
    pd.to_numeric(master_orders.loc[mask_1p_admin, 'Taxes'], errors='coerce').fillna(0)
)
print(f"   ✅ Admin tax taken from 'Taxes' column directly")

# ── B2B/B2C Zoho: Sum CGST + SGST + IGST (line-level)
zoho_tax = pd.Series(0.0, index=master_orders.index)
for tax_col in ['CGST', 'SGST', 'IGST']:
    if tax_col in master_orders.columns:
        zoho_tax = zoho_tax + pd.to_numeric(master_orders[tax_col], errors='coerce').fillna(0)
        print(f"   ✅ Added {tax_col} to Zoho tax")
    else:
        print(f"   ⚠️  Column '{tax_col}' not found — skipping")

master_orders.loc[mask_b2b_zoho, 'Tax_Amount'] = zoho_tax[mask_b2b_zoho]

# Round Tax_Amount
master_orders['Tax_Amount'] = master_orders['Tax_Amount'].round(2)

# ── Calculate NEW Net_Revenue = Gross_Revenue - Tax_Amount
master_orders['Net_Revenue'] = (master_orders['Gross_Revenue'] - master_orders['Tax_Amount']).round(2)

# Report
print(f"\n📊 Tax summary by channel:")
for ch in ['1P', 'B2B', 'B2C']:
    ch_mask = master_orders['channel'] == ch
    if ch_mask.sum() > 0:
        g = master_orders.loc[ch_mask, 'Gross_Revenue'].sum()
        t = master_orders.loc[ch_mask, 'Tax_Amount'].sum()
        n = master_orders.loc[ch_mask, 'Net_Revenue'].sum()
        print(f"   {ch:5s}: Gross ₹{g:>14,.0f} | Tax ₹{t:>12,.0f} | Net ₹{n:>14,.0f}")

print(f"\n✅ FINAL REVENUE COLUMNS:")
print(f"   Gross_Revenue (with tax):   ₹{master_orders['Gross_Revenue'].sum():,.2f}")
print(f"   Tax_Amount:                 ₹{master_orders['Tax_Amount'].sum():,.2f}")
print(f"   Net_Revenue (without tax):  ₹{master_orders['Net_Revenue'].sum():,.2f}")

# Sanity check
diff = (master_orders['Gross_Revenue'].sum() - master_orders['Tax_Amount'].sum() - master_orders['Net_Revenue'].sum())
print(f"\n   Math check: Gross - Tax - Net = ₹{diff:.2f} {'✅' if abs(diff) < 1 else '❌'}")



# ---- notebook cell 24 --------------------------------------------------
def normalize_status(value):
    if pd.isna(value) or value is None:
        return ''
    text = str(value).lower().strip()
    text = re.sub(r'[\s\-_]+', '_', text)
    text = re.sub(r'[^a-z0-9_]', '', text)
    return text

def match_keyword(text, keywords):
    return any(kw in text for kw in keywords)

FINANCIAL_KEYWORDS = {
    'paid'              : ['paid', 'success', 'completed', 'complete'],
    'pending'           : ['pending', 'awaiting', 'wait', 'hold'],
    'refunded'          : ['refund', 'refunded', 'returned'],
    'partially_refunded': ['partial_refund', 'partially_refunded', 'part_refund'],
    'voided'            : ['void', 'voided', 'canceled_payment'],
}

FULFILLMENT_KEYWORDS = {
    'fulfilled'   : ['fulfill', 'fulfilled', 'shipped', 'delivered', 'complete'],
    'unfulfilled' : ['unfulfilled', 'not_fulfilled', 'unfullfilled', 'pending'],
    'partial'     : ['partial', 'partially', 'part'],
    'cancelled'   : ['cancel', 'cancelled', 'canceled', 'void'],
    'restocked'   : ['restock', 'restocked', 'returned_to_inventory'],
}

status_mapping = {
    ('paid',               'fulfilled')   : 'Completed',
    ('paid',               'unfulfilled') : 'Completed',
    ('paid',               'partial')     : 'Completed',
    ('pending',            'fulfilled')   : 'Completed',
    ('pending',            'unfulfilled') : 'Pending',
    ('pending',            'partial')     : 'Pending',
    ('pending',            'cancelled')   : 'Cancelled',
    ('paid',               'cancelled')   : 'Cancelled',
    ('refunded',           'fulfilled')   : 'Refunded',
    ('refunded',           'unfulfilled') : 'Refunded',
    ('partially_refunded', 'fulfilled')   : 'Refunded',
    ('partially_refunded', 'unfulfilled') : 'Refunded',
    ('paid',               'restocked')   : 'Refunded',
    ('pending',            'restocked')   : 'Refunded',
    ('voided',             'fulfilled')   : 'Voided',
    ('voided',             'unfulfilled') : 'Voided',
}

def standardize_financial(text):
    text = normalize_status(text)
    for std_status, keywords in FINANCIAL_KEYWORDS.items():
        if match_keyword(text, keywords):
            return std_status
    return text

def standardize_fulfillment(text):
    text = normalize_status(text)
    # GUARD: 'unfulfilled' contains the substring 'fulfill', so naive substring
    # matching wrongly tags it as 'fulfilled'. Catch the negative case FIRST.
    if 'unfulfill' in text or 'unfullfill' in text or 'not_fulfill' in text:
        return 'unfulfilled'
    for std_status, keywords in FULFILLMENT_KEYWORDS.items():
        if match_keyword(text, keywords):
            return std_status
    return text

def assign_order_status(row):
    # B2B: use Invoice Status (now in Fulfillment Status)
    # Per spec: open / overdue / fulfilled / unfulfilled / paid / partially_paid → Completed
    #          void / voided → Voided
    #          cancelled / canceled → Cancelled
    if str(row['channel']).lower() in ['b2b', 'b2c']:
        status = str(row.get('Fulfillment Status', '')).strip().lower()
        if status in ('void', 'voided'):
            return 'Voided'
        if status in ('cancel', 'cancelled', 'canceled'):
            return 'Cancelled'
        # Everything else (open, overdue, fulfilled, unfulfilled, paid, etc.) → Completed
        return 'Completed'
    
    # 1P fuzzy mapping
    financial   = standardize_financial(row['Financial Status'])
    fulfillment = standardize_fulfillment(row['Fulfillment Status'])
    
    status = status_mapping.get((financial, fulfillment))
    if status:
        return status
    
    if 'refund' in financial:
        return 'Refunded'
    if 'cancel' in fulfillment or 'void' in financial:
        return 'Cancelled'
    if financial == 'pending' and fulfillment == 'fulfilled':
        return 'Completed'
    if financial in ('paid', 'pending'):
        return 'Completed' if fulfillment == 'fulfilled' else 'Pending'
    return 'Other'

master_orders['order_status'] = master_orders.apply(assign_order_status, axis=1)

print(f"✅ Order status mapped")
print(master_orders['order_status'].value_counts().to_string())

# Show B2B breakdown specifically
print(f"\n📋 B2B (Zoho) status breakdown:")
zoho = master_orders[master_orders['sub_channel'] == 'Zoho_Invoice']
print(zoho.groupby(['Fulfillment Status', 'order_status']).size().to_string())


# ---- notebook cell 25 --------------------------------------------------
# v7: Adjust Gross_Revenue, Tax_Amount, Net_Revenue all together based on order_status

print("💰 Adjusting revenues based on order_status...")

# Preserve originals for reference
master_orders['Gross_Revenue_Original'] = master_orders['Gross_Revenue'].copy()
master_orders['Net_Revenue_Original']   = master_orders['Net_Revenue'].copy()
master_orders['Tax_Amount_Original']    = master_orders['Tax_Amount'].copy()

# ── Refunded → negate all three (revenue went OUT)
refund_mask = master_orders['order_status'] == 'Refunded'
n_refund = refund_mask.sum()
if n_refund > 0:
    master_orders.loc[refund_mask, 'Gross_Revenue'] = -master_orders.loc[refund_mask, 'Gross_Revenue'].abs()
    master_orders.loc[refund_mask, 'Tax_Amount']    = -master_orders.loc[refund_mask, 'Tax_Amount'].abs()
    master_orders.loc[refund_mask, 'Net_Revenue']   = -master_orders.loc[refund_mask, 'Net_Revenue'].abs()
    print(f"   ❌ Refunded → negative:  {n_refund:>5,} rows")
    print(f"      Net Impact: ₹{master_orders.loc[refund_mask, 'Net_Revenue'].sum():,.2f}")
else:
    print(f"   ❌ Refunded → negative:  0 rows")

# ── Voided → zero out all three
void_mask = master_orders['order_status'] == 'Voided'
n_void = void_mask.sum()
if n_void > 0:
    master_orders.loc[void_mask, ['Gross_Revenue', 'Tax_Amount', 'Net_Revenue']] = 0.0
    print(f"   🚫 Voided     → zero:      {n_void:>5,} rows")
else:
    print(f"   🚫 Voided     → zero:      0 rows")

# Round to be safe
for col in ['Gross_Revenue', 'Tax_Amount', 'Net_Revenue']:
    master_orders[col] = master_orders[col].round(2)

print(f"\n📊 Revenue summary by order_status:")
summary = master_orders.groupby('order_status').agg(
    rows=('Net_Revenue', 'count'),
    gross_revenue=('Gross_Revenue', 'sum'),
    tax_amount=('Tax_Amount', 'sum'),
    net_revenue=('Net_Revenue', 'sum'),
).round(2)
print(summary.to_string())

print(f"\n✅ FINAL TOTALS (after status adjustment):")
print(f"   Gross_Revenue: ₹{master_orders['Gross_Revenue'].sum():,.2f}")
print(f"   Tax_Amount:    ₹{master_orders['Tax_Amount'].sum():,.2f}")
print(f"   Net_Revenue:   ₹{master_orders['Net_Revenue'].sum():,.2f}")



# ---- notebook cell 26 --------------------------------------------------
print("🕐 Adding Order Hour + Order Time Period columns...")

# Ensure datetime
master_orders['Created at'] = pd.to_datetime(master_orders['Created at'], errors='coerce')

# Order Hour (0–23, NaN if Created at is NaT)
master_orders['Order Hour'] = master_orders['Created at'].dt.hour

# Order Time Period — split at 3:30 PM (15:30)
total_minutes = (
    master_orders['Created at'].dt.hour * 60 +
    master_orders['Created at'].dt.minute
)
master_orders['Order Time Period'] = np.where(
    total_minutes.isna(),
    None,
    np.where(total_minutes < 930, 'Before 3:30 PM', 'After 3:30 PM')
)

# Report
n_filled = master_orders['Order Time Period'].notna().sum()
print(f"   ✅ Filled {n_filled:,} / {len(master_orders):,} rows ({n_filled/len(master_orders)*100:.1f}%)")

print(f"\n📊 Order Time Period distribution:")
print(master_orders['Order Time Period'].value_counts(dropna=False).to_string())

print(f"\n⏰ Top 5 order hours (peak browsing/ordering):")
print(master_orders['Order Hour'].value_counts().head(5).sort_index().to_string())


# ---- notebook cell 27 --------------------------------------------------
print("🔄 Adding segmentation columns...")

# ⚡ FIX: Only consider Completed orders for segmentation
# Voided/Cancelled/Refunded shouldn't count toward customer's lifetime journey
valid_orders = master_orders[
    master_orders['customer_key'].notna() &
    (master_orders['order_status'] == 'Completed')   # ← NEW filter
].copy()

order_level = (
    valid_orders.groupby(['customer_key', 'Name'])
    .agg(order_date=('Created at', 'min'))
    .reset_index()
)
order_level = order_level.sort_values(['customer_key', 'order_date'])
order_level['lifetime_order_no'] = order_level.groupby('customer_key').cumcount() + 1

def tag_segment(seq):
    if seq == 1:   return 'NCA'
    elif seq == 2: return 'RC2'
    elif seq == 3: return 'RC3'
    else:          return 'LC'

order_level['segment_dynamic'] = order_level['lifetime_order_no'].apply(tag_segment)

customer_totals = (
    order_level.groupby('customer_key')
    .agg(total_lifetime_orders=('Name', 'nunique'))
    .reset_index()
)
customer_totals['segment_lifetime'] = customer_totals['total_lifetime_orders'].apply(tag_segment)

cols_to_drop = ['lifetime_order_no', 'segment_dynamic', 'total_lifetime_orders', 'segment_lifetime']
master_orders = master_orders.drop(columns=[c for c in cols_to_drop if c in master_orders.columns])

master_orders = master_orders.merge(
    order_level[['customer_key', 'Name', 'lifetime_order_no', 'segment_dynamic']],
    on=['customer_key', 'Name'], how='left'
)
master_orders = master_orders.merge(
    customer_totals[['customer_key', 'total_lifetime_orders', 'segment_lifetime']],
    on='customer_key', how='left'
)

master_orders['lifetime_order_no']     = master_orders['lifetime_order_no'].fillna(0).astype(int)
master_orders['segment_dynamic']       = master_orders['segment_dynamic'].fillna('NO_PHONE')
master_orders['total_lifetime_orders'] = master_orders['total_lifetime_orders'].fillna(0).astype(int)
master_orders['segment_lifetime']      = master_orders['segment_lifetime'].fillna('NO_PHONE')

print(f"✅ Segmentation added (Completed orders only)")
print(f"\n📊 segment_dynamic:\n{master_orders['segment_dynamic'].value_counts().to_string()}")


# ---- notebook cell 28 --------------------------------------------------
print("🔄 Calculating transitions...")

cols_to_drop = [col for col in master_orders.columns
                if 'transition' in col.lower() or col in ['prev_segment', 'prev_order_date']]
if cols_to_drop:
    master_orders = master_orders.drop(columns=cols_to_drop)

# ⚡ FIX: Only consider Completed orders for transitions
unique_orders = master_orders[
    master_orders['order_status'] == 'Completed'      # ← NEW filter
].drop_duplicates(subset='Name')[
    ['customer_key', 'Name', 'Created at', 'segment_dynamic']
].copy()

unique_orders = unique_orders.sort_values(['customer_key', 'Created at']).reset_index(drop=True)
unique_orders['prev_segment']    = unique_orders.groupby('customer_key')['segment_dynamic'].shift(1)
unique_orders['prev_order_date'] = unique_orders.groupby('customer_key')['Created at'].shift(1)

def get_transition(row):
    if pd.isna(row['prev_segment']):
        return "NCA"
    prev_seg = row['prev_segment']
    curr_seg = row['segment_dynamic']
    same_day = (
        pd.to_datetime(row['Created at']).date() ==
        pd.to_datetime(row['prev_order_date']).date()
    )
    if same_day and prev_seg != curr_seg:
        return "SameDay_Transition"
    if prev_seg == "NCA" and curr_seg == "RC2":
        return "NCA→RC2"
    elif prev_seg == "RC2" and curr_seg == "RC3":
        return "RC2→RC3"
    elif prev_seg == "RC3" and curr_seg == "LC":
        return "RC3→LC"
    elif curr_seg == "LC":
        return "Stayed_LC"
    return "Stayed_LC"

unique_orders['transition_tag'] = unique_orders.apply(get_transition, axis=1)
unique_orders['transition_days'] = (
    pd.to_datetime(unique_orders['Created at']) -
    pd.to_datetime(unique_orders['prev_order_date'])
).dt.days.fillna(0).astype(int)

master_orders = master_orders.merge(
    unique_orders[['Name', 'transition_tag', 'transition_days']],
    on='Name', how='left'
)

print(f"✅ Transitions done (Completed orders only)")
print(master_orders['transition_tag'].value_counts().to_string())


# ---- notebook cell 29 --------------------------------------------------
# Test Orders
test_mask = (
    master_orders['Full Name'].astype(str).str.contains('test', case=False, na=False) |
    master_orders['Tags_customer'].astype(str).str.contains('test', case=False, na=False) |
    master_orders['Lineitem name'].astype(str).str.contains('test', case=False, na=False)
)
master_orders.loc[test_mask, 'order_status'] = 'Test Orders'
print(f"✅ Marked {test_mask.sum()} test orders")

# Internal Transfer
internal_mask = (
    master_orders['Full Name'].astype(str).str.contains('transfer', case=False, na=False) |
    master_orders['Tags_customer'].astype(str).str.contains('transfer', case=False, na=False) |
    master_orders['Lineitem name'].astype(str).str.contains('transfer', case=False, na=False)
)
master_orders.loc[internal_mask, 'order_status'] = 'Internal Transfer'
print(f"✅ Marked {internal_mask.sum()} internal transfer orders")

# Duplicate Orders
duplicate_mask = (
    master_orders['Full Name'].astype(str).str.contains('duplicate', case=False, na=False) |
    master_orders['Tags_customer'].astype(str).str.contains('duplicate', case=False, na=False) |
    master_orders['Lineitem name'].astype(str).str.contains('duplicate', case=False, na=False)
)
master_orders.loc[duplicate_mask, 'order_status'] = 'Duplicate Orders'
print(f"✅ Marked {duplicate_mask.sum()} duplicate orders")

# Discarded Orders
discarded_mask = (
    master_orders['Full Name'].astype(str).str.contains('discarded', case=False, na=False) |
    master_orders['Tags_customer'].astype(str).str.contains('discarded', case=False, na=False) |
    master_orders['Lineitem name'].astype(str).str.contains('discarded', case=False, na=False)
)
master_orders.loc[discarded_mask, 'order_status'] = 'Discarded'
print(f"✅ Marked {discarded_mask.sum()} discarded orders")

print(master_orders['order_status'].value_counts().to_string())


# ---- notebook cell 30 --------------------------------------------------
zip_admin_sql = """
SELECT 
    o.order_id, 
    ca.address, 
    ca.pincode 
FROM orders o 
JOIN customer_addresses ca   
    ON o.address_id = ca.address_id
"""

print("Loading zip_admin... (30-60 sec)")
zip_admin = pd.read_sql(zip_admin_sql, engine)
print(f"✅ Loaded zip_admin: {len(zip_admin):,} rows × {len(zip_admin.columns)} columns")
print(f"\nSample:")
print(zip_admin.head(3).to_string(index=False))


# ---- notebook cell 31 --------------------------------------------------
# ── 15.1: Helper functions

def clean_zip(val):
    """Clean a zip value to a 6-digit string. Returns None if invalid."""
    if pd.isna(val):
        return None
    s = str(val).strip().replace("'", "").replace(" ", "")
    if s.lower() in ['nan', 'none', '', 'null', '<na>']:
        return None
    if s.endswith('.0'):
        s = s[:-2]
    if 'e' in s.lower():
        try:
            s = str(int(float(s)))
        except Exception:
            return None
    s = re.sub(r'\D', '', s)
    if len(s) == 6 and s.isdigit():
        return s
    return None


def clean_order_name(val):
    """Normalize order name for matching (#DN20645, '5617', etc.)."""
    if pd.isna(val):
        return None
    s = str(val).strip().lstrip("'").strip()
    if s.lower() in ['nan', 'none', '', 'null']:
        return None
    return s


print("✅ Helpers loaded")


# ---- notebook cell 32 --------------------------------------------------
# ── 15.2: Build the Indian pincode → (city, area, district, state) lookup
#         Indian_zip_codes columns: City, Area, Pincode, District, State

print("🗺️  Building pincode → location lookup from Indian_zip_codes...")

izc = Indian_zip_codes.copy()

# Normalize column names (handle variations)
izc.columns = [c.strip() for c in izc.columns]
print(f"   Columns: {izc.columns.tolist()}")

# Standardize column names
rename_map = {}
for c in izc.columns:
    cl = c.lower()
    if 'pincode' in cl or 'pin' in cl or 'zip' in cl:
        rename_map[c] = 'pincode'
    elif 'city' in cl:
        rename_map[c] = 'city'
    elif 'area' in cl or 'post' in cl or 'office' in cl:
        rename_map[c] = 'area'
    elif 'district' in cl:
        rename_map[c] = 'district'
    elif 'state' in cl or 'region' in cl:
        rename_map[c] = 'state'
izc = izc.rename(columns=rename_map)

# Make sure all 5 columns exist (some Kaggle datasets vary)
for col in ['pincode', 'city', 'area', 'district', 'state']:
    if col not in izc.columns:
        izc[col] = None
        print(f"   ⚠️  Column '{col}' not found in Indian_zip_codes — will be blank")

# Clean pincode
izc['pincode'] = izc['pincode'].apply(clean_zip)
izc = izc[izc['pincode'].notna()]

# For each pincode, aggregate:
#  - city/district/state: take MOST COMMON (mode) value
#  - area: take FIRST area alphabetically (deterministic)
def safe_mode(s):
    s = s.dropna().astype(str).str.strip()
    s = s[s != '']
    if len(s) == 0:
        return None
    return s.mode().iloc[0]


pincode_lookup = izc.groupby('pincode').agg(
    city     = ('city',     safe_mode),
    state    = ('state',    safe_mode),
    district = ('district', safe_mode),
    area     = ('area',     lambda x: x.dropna().astype(str).str.strip().sort_values().iloc[0] if len(x.dropna()) > 0 else None),
).reset_index()

# Title-case for consistency
for col in ['city', 'state', 'district', 'area']:
    pincode_lookup[col] = pincode_lookup[col].apply(
        lambda v: str(v).strip().title() if pd.notna(v) else None
    )

# Convert to dict for fast lookup: pincode → {city, area, district, state}
PINCODE_LOOKUP = pincode_lookup.set_index('pincode').to_dict('index')
print(f"   ✅ Built lookup for {len(PINCODE_LOOKUP):,} unique pincodes")
print(f"   Sample (110017): {PINCODE_LOOKUP.get('110017', 'NOT FOUND')}")
print(f"   Sample (122001): {PINCODE_LOOKUP.get('122001', 'NOT FOUND')}")


# ---- notebook cell 33 --------------------------------------------------
# ── 15.3: Build SHOPIFY order → pincode lookup (one-time, from zip_shopify CSV)
#         zip_shopify columns: Order name, Shipping city, Shipping region, Shipping postal code

print("\n📦 Building Shopify order → pincode lookup...")

zsh = zip_shopify.copy()
zsh.columns = [c.strip() for c in zsh.columns]
print(f"   Columns: {zsh.columns.tolist()}")

# Standardize column names
rename_map = {}
for c in zsh.columns:
    cl = c.lower()
    if 'order' in cl and ('name' in cl or 'id' in cl):
        rename_map[c] = 'order_name'
    elif 'postal' in cl or 'pincode' in cl or 'zip' in cl:
        rename_map[c] = 'postal_code'
    elif 'city' in cl:
        rename_map[c] = 'shipping_city'
    elif 'region' in cl or 'state' in cl or 'province' in cl:
        rename_map[c] = 'shipping_region'
zsh = zsh.rename(columns=rename_map)

# Clean
zsh['order_name'] = zsh['order_name'].apply(clean_order_name)
zsh['postal_code'] = zsh['postal_code'].apply(clean_zip)

# Drop duplicates (one row per order_name)
zsh = zsh.drop_duplicates(subset='order_name', keep='first')
zsh = zsh[zsh['order_name'].notna() & zsh['postal_code'].notna()]

# Build dict: order_name → postal_code
SHOPIFY_ZIP_LOOKUP = dict(zip(zsh['order_name'], zsh['postal_code']))
print(f"   ✅ Built Shopify lookup: {len(SHOPIFY_ZIP_LOOKUP):,} order_name → pincode mappings")


# ---- notebook cell 34 --------------------------------------------------
# ── 15.4: Build ADMIN order → pincode lookup (fresh from DB each run)
#         zip_admin columns: order_id, order_name, address, pincode

print("\n📦 Building Admin order → pincode lookup...")

za = zip_admin.copy()
za.columns = [c.strip() for c in za.columns]
print(f"   Columns: {za.columns.tolist()}")

# Standardize
if 'order_name' in za.columns:
    za['order_name'] = za['order_name'].apply(clean_order_name)
else:
    # If order_name isn't in the SQL output, fall back to using order_id as string
    print("   ⚠️  'order_name' not in zip_admin — using order_id (str) as fallback match key")
    za['order_name'] = za['order_id'].astype(str).str.strip()

za['pincode'] = za['pincode'].apply(clean_zip)

# Drop duplicates
za = za.drop_duplicates(subset='order_name', keep='first')
za = za[za['order_name'].notna() & za['pincode'].notna()]

# Build dict
ADMIN_ZIP_LOOKUP = dict(zip(za['order_name'], za['pincode']))
print(f"   ✅ Built Admin lookup: {len(ADMIN_ZIP_LOOKUP):,} order_name → pincode mappings")
print(f"   Sample entries: {dict(list(ADMIN_ZIP_LOOKUP.items())[:3])}")


# ---- notebook cell 35 --------------------------------------------------
# ── 15.5: Resolve zip per row using the right lookup per sub_channel

print("\n🔧 Resolving zip per row from sub_channel-specific lookups...")

def resolve_zip(row):
    """
    Returns the cleanest pincode for this row:
      - Shopify sub_channels: look up Name in SHOPIFY_ZIP_LOOKUP
      - Admin / B2B_DraftOrder sub_channels: look up Name in ADMIN_ZIP_LOOKUP
      - Fallback: existing Shipping Zip / Billing Zip / Zip columns
    """
    sub = str(row.get('sub_channel', '')).strip()
    name = clean_order_name(row.get('Name'))

    # SHOPIFY sub-channels
    if sub.startswith('Shopify') and name:
        v = SHOPIFY_ZIP_LOOKUP.get(name)
        if v:
            return v

    # ADMIN sub-channels (and B2B_DraftOrder, which originated from admin)
    if (sub.startswith('Admin') or sub == 'B2B_DraftOrder') and name:
        v = ADMIN_ZIP_LOOKUP.get(name)
        if v:
            return v

    # Fallback: existing zip columns from the raw exports
    for col in ['Shipping Zip', 'Billing Zip', 'Default Address Zip', 'Zip']:
        if col in row.index:
            v = clean_zip(row.get(col))
            if v:
                return v
    return None


# Apply (vectorized via apply on rows — fast enough for ~35K rows)
master_orders['zip'] = master_orders.apply(resolve_zip, axis=1)

n_filled = master_orders['zip'].notna().sum()
print(f"   ✅ zip filled: {n_filled:,} / {len(master_orders):,} ({n_filled/len(master_orders)*100:.1f}%)")

# Breakdown by sub_channel
print(f"\n   📊 Coverage by sub_channel:")
breakdown = master_orders.groupby('sub_channel').agg(
    total=('zip', 'count'),
    with_zip=('zip', lambda x: x.notna().sum()),
).reset_index()
breakdown['coverage_pct'] = (breakdown['with_zip'] / breakdown['total'] * 100).round(1)
print(breakdown.to_string(index=False))


# ---- notebook cell 36 --------------------------------------------------
# ── 15.6: Look up city / area / district / state from Indian pincode dictionary

print("\n🏙️  Looking up city/area/district/state from Indian_zip_codes...")

def lookup_location(zipcode):
    """Returns (city, area, district, state) tuple for a pincode."""
    if pd.isna(zipcode):
        return (None, None, None, None)
    entry = PINCODE_LOOKUP.get(str(zipcode))
    if not entry:
        return (None, None, None, None)
    return (entry.get('city'), entry.get('area'), entry.get('district'), entry.get('state'))


# Apply in one pass
results = master_orders['zip'].apply(lookup_location)
master_orders['city']     = results.apply(lambda x: x[0])
master_orders['area']     = results.apply(lambda x: x[1])
master_orders['district'] = results.apply(lambda x: x[2])
master_orders['state']    = results.apply(lambda x: x[3])

# Reporting
print(f"\n   ✅ Five address columns ready:")
for col in ['zip', 'city', 'area', 'district', 'state']:
    n = master_orders[col].notna().sum()
    print(f"      {col:9s}: {n:>6,} / {len(master_orders):,} ({n/len(master_orders)*100:.1f}%)")

# Top cities / states for sanity check
print(f"\n   🏙️  Top 15 cities:")
print(master_orders['city'].value_counts().head(15).to_string())

print(f"\n   📊 Top 10 states:")
print(master_orders['state'].value_counts().head(10).to_string())

# Unresolved rows
unresolved = master_orders[master_orders['zip'].isna()]
print(f"\n   ❓ Rows with no zip ({len(unresolved):,}):")
if len(unresolved) > 0:
    print(f"      By sub_channel:")
    print(unresolved['sub_channel'].value_counts().to_string())


# ---- notebook cell 37 --------------------------------------------------
COLS_TO_DROP = [
    # Address columns (replaced by city/state/zip)
    'Billing Address1', 'Billing City', 'Billing Zip',
    'Billing Province Name', 'Billing Country',
    'Shipping Address1', 'Shipping City', 'Shipping Zip',
    'Shipping Province Name', 'Shipping Country',
    'Shipping Province',
    # Default address (customer-level — redundant)
    'Default Address Address1', 'Default Address City',
    'Default Address Zip', 'Default Address Country Code',
    # Standalone Zip from admin
    'Zip',
    # Duplicate price column
    'Lineitem unit price',
    # Risk Level (per handoff)
    'Risk Level',
]

cols_present = [c for c in COLS_TO_DROP if c in master_orders.columns]
master_orders = master_orders.drop(columns=cols_present)

print(f"✅ Dropped {len(cols_present)} redundant columns:")
for c in cols_present:
    print(f"   − {c}")
print(f"\n   Final shape: {master_orders.shape[0]:,} rows × {master_orders.shape[1]} columns")


# ---- notebook cell 38 --------------------------------------------------
print("🧹 Cleaning for Power BI refresh...")

# Fix 1: Replace 'nan' text strings
for col in master_orders.columns:
    if master_orders[col].dtype == 'object':
        master_orders[col] = master_orders[col].astype(str).str.replace('_nan', '_', regex=False)
        master_orders[col] = master_orders[col].replace('nan', '')
        master_orders[col] = master_orders[col].replace('None', '')
        master_orders[col] = master_orders[col].replace('NaT', '')

# Fix 2: Numeric cols (v7: added Tax_Amount, Gross_Revenue, Net_Revenue, CGST/SGST/IGST)
numeric_cols = [
    'Subtotal', 'Shipping', 'Taxes', 'Total', 'Discount Amount',
    'Lineitem quantity', 'Lineitem price', 'Refunded Amount',
    'Outstanding Balance', 'Lineitem discount',
    'Lineitem_Revenue', 'Lineitem_Discount', 'Lineitem_Shipping',
    'Tax_Amount',                                       # v7 NEW
    'Gross_Revenue', 'Net_Revenue',                     # v7 NEW (renamed/redefined)
    'Gross_Revenue_Original', 'Net_Revenue_Original',   # v7 NEW (originals preserved)
    'Tax_Amount_Original',                              # v7 NEW
    'Discount_Ratio',
    'CGST', 'SGST', 'IGST',                             # v7 NEW (Zoho tax components)
]
for col in numeric_cols:
    if col in master_orders.columns:
        master_orders[col] = pd.to_numeric(master_orders[col], errors='coerce').fillna(0)

# Fix 3: Date cols
date_cols = ['Created at', 'Paid at', 'Fulfilled at', 'Cancelled at', 'Delivery Date']
for col in date_cols:
    if col in master_orders.columns:
        master_orders[col] = pd.to_datetime(master_orders[col], errors='coerce')

# Fix 4: Strip whitespace
for col in master_orders.select_dtypes(include='object').columns:
    master_orders[col] = master_orders[col].astype(str).str.strip()

print(f"✅ Cleaning complete: {master_orders.shape}")

# v7: Verify new columns present
print(f"\n📋 v7 NEW columns check:")
for col in ['Tax_Amount', 'Gross_Revenue', 'Net_Revenue']:
    if col in master_orders.columns:
        non_zero = (master_orders[col] != 0).sum()
        total = master_orders[col].sum()
        print(f"   {col:25s}: {non_zero:>6,} non-zero | Total ₹{total:>14,.2f}")
    else:
        print(f"   {col:25s}: ❌ MISSING!")



# ---- notebook cell 39 --------------------------------------------------
before = len(master_orders)
unique_orders_before = master_orders['Name'].nunique()
print(f"📊 BEFORE dedup: {before:,} rows, {unique_orders_before:,} unique orders")

dup_count = master_orders.duplicated(keep='first').sum()
print(f"\n🔍 100% identical rows detected: {dup_count}")

master_orders = master_orders.drop_duplicates(keep='first').reset_index(drop=True)

after = len(master_orders)
unique_orders_after = master_orders['Name'].nunique()
print(f"\n📊 AFTER dedup: {after:,} rows, {unique_orders_after:,} unique orders")
print(f"   Removed: {before - after} identical rows")
print(f"   Orders preserved: {unique_orders_after}/{unique_orders_before}")


# ---- notebook cell 40 --------------------------------------------------
# 1. Lifetime first order date per customer (immutable per customer)
first_order = (
    master_orders[master_orders['customer_key'].notna()]
    .groupby('customer_key')['Created at']
    .min()
    .rename('first_order_date')
)
master_orders = master_orders.merge(
    first_order, left_on='customer_key', right_index=True, how='left'
)

print(f"✅ first_order_date added")
print(f"   Coverage: {master_orders['first_order_date'].notna().sum():,} / {len(master_orders):,} rows")
print(f"   (total_lifetime_orders already exists from Section 12)")


# 2. Lifetime last order date per customer
last_order = (
    master_orders[master_orders['customer_key'].notna()]
    .groupby('customer_key')['Created at']
    .max()
    .rename('last_order_date')
)
master_orders = master_orders.merge(
    last_order, left_on='customer_key', right_index=True, how='left'
)
print(f"✅ last_order_date added")
print(f"   Coverage: {master_orders['last_order_date'].notna().sum():,} / {len(master_orders):,} rows")

# 3. Days since last order (today - last order date)
master_orders['days_since_last_order'] = (
    pd.Timestamp.today().normalize() - master_orders['last_order_date']
).dt.days

print(f"✅ days_since_last_order added")
print(f"   Coverage: {master_orders['days_since_last_order'].notna().sum():,} / {len(master_orders):,} rows")
print(f"   Min: {master_orders['days_since_last_order'].min()} days")
print(f"   Max: {master_orders['days_since_last_order'].max()} days")

# 4. First order amount (sum of revenue from customer's first order)
first_order_amount = (
    master_orders[master_orders['customer_key'].notna()]
    .groupby('customer_key')
    .apply(lambda x: x[x['Created at'] == x['Created at'].min()]['Net_Revenue'].sum())
    .rename('first_order_amount')
)
master_orders = master_orders.merge(
    first_order_amount, left_on='customer_key', right_index=True, how='left'
)
print(f"✅ first_order_amount added")
print(f"   Coverage: {master_orders['first_order_amount'].notna().sum():,} / {len(master_orders):,} rows")
print(f"   Min: ₹{master_orders['first_order_amount'].min():,.0f}")
print(f"   Max: ₹{master_orders['first_order_amount'].max():,.0f}")
print(f"   Mean: ₹{master_orders['first_order_amount'].mean():,.0f}")


# 5. Last order amount (sum of revenue from customer's last order)
last_order_amount = (
    master_orders[master_orders['customer_key'].notna()]
    .groupby('customer_key')
    .apply(lambda x: x[x['Created at'] == x['Created at'].max()]['Net_Revenue'].sum())
    .rename('last_order_amount')
)
master_orders = master_orders.merge(
    last_order_amount, left_on='customer_key', right_index=True, how='left'
)
print(f"✅ last_order_amount added")
print(f"   Coverage: {master_orders['last_order_amount'].notna().sum():,} / {len(master_orders):,} rows")
print(f"   Min: ₹{master_orders['last_order_amount'].min():,.0f}")
print(f"   Max: ₹{master_orders['last_order_amount'].max():,.0f}")
print(f"   Mean: ₹{master_orders['last_order_amount'].mean():,.0f}")


# ---- notebook cell 41 --------------------------------------------------
def clean_name(name):
    """
    Returns a cleaned, title-cased name OR None for invalid/junk values.
    Filters out: nulls, whitespace-only, common placeholder garbage ('-', 'NA', etc.)
    """
    if pd.isna(name):
        return None
    s = str(name).strip()
    
    # Strip leading apostrophe (defensive — Excel/Sheets text-force prefix)
    while s.startswith("'"):
        s = s[1:].strip()
    
    # Common garbage placeholders that should be treated as blank
    if s.lower() in [
        '', 'nan', 'none', 'null', '<na>', 'n/a', 'na',
        '-', '--', '---', '—',
        'test', 'guest', 'customer',
    ]:
        return None
    
    # Strip stray punctuation-only strings
    if not any(c.isalpha() for c in s):
        return None
    
    # Title case for consistency (Admin/B2B already title-cased, Shopify often isn't)
    return s.title()
 
 
def coalesce_full_name(row):
    """Priority: Full Name → Billing Name → Shipping Name"""
    for col in ['Full Name', 'Billing Name', 'Shipping Name']:
        if col in row.index:
            v = clean_name(row.get(col))
            if v:
                return v
    return None
 
 
# ── Diagnostic: count blanks BEFORE the fix
before_blank = master_orders['Full Name'].isna().sum() + (
    master_orders['Full Name'].astype(str).str.strip().isin(['', 'nan', 'None']).sum()
)
print(f"📋 Full Name BEFORE fallback:")
print(f"   Blank: {before_blank:,} / {len(master_orders):,} rows")
 
# ── Detect which fallback sources are available
name_cols_available = [c for c in ['Full Name', 'Billing Name', 'Shipping Name']
                       if c in master_orders.columns]
print(f"   Fallback sources available: {name_cols_available}")
 
# ── Apply the coalesce
master_orders['Full Name'] = master_orders.apply(coalesce_full_name, axis=1)
 
# ── Diagnostic: count blanks AFTER the fix
after_blank = master_orders['Full Name'].isna().sum()
 
print(f"\n✅ Full Name AFTER fallback:")
print(f"   Filled: {master_orders['Full Name'].notna().sum():,} / {len(master_orders):,} rows")
print(f"   Still blank: {after_blank:,}")
print(f"   Recovered via fallback: {before_blank - after_blank:,}")
 
# ── Show source breakdown (which column the final value came from)
def trace_source(row):
    """Track which column the final Full Name was sourced from"""
    final = clean_name(row.get('Full Name'))
    if not final:
        return 'still_blank'
    # Check original sources
    for col in ['Billing Name', 'Shipping Name']:
        if col in row.index and clean_name(row.get(col)) == final:
            # Could be from this source — check if it's the priority winner
            return col
    return 'Full Name'  # default — came from original Full Name
 
# Light diagnostic — only run on a sample to avoid performance hit on 35K+ rows
sample_size = min(5000, len(master_orders))
sample = master_orders.sample(sample_size, random_state=42)
source_breakdown = sample.apply(trace_source, axis=1).value_counts()
print(f"\n📊 Source breakdown (random sample of {sample_size:,}):")
for src, n in source_breakdown.items():
    pct = n / sample_size * 100
    print(f"   {src:20s}: {n:>5,} ({pct:>5.1f}%)")
 
# ── Sample of rows that are STILL blank (for investigation)
if after_blank > 0:
    still_blank = master_orders[master_orders['Full Name'].isna()][
        ['Name', 'channel', 'sub_channel', 'Billing Name', 'Shipping Name', 'customer_key']
    ].head(10)
    print(f"\n🔍 Sample of rows STILL blank (no name found anywhere):")
    print(still_blank.to_string(index=False))
 


# ---- notebook cell 42 --------------------------------------------------
# ============================================================
#  PATCH: fix order_status when the order was actually cancelled
#  Paste BEFORE pushing master_orders to the sheet.
#  Edits master_orders in place — final df stays master_orders.
# ============================================================

CANCELLED_COL = "Cancelled at"    # exact column name
STATUS_COL    = "order_status"    # exact column name

# A row counts as "cancelled" if Cancelled at is not blank/NaN/empty string
cancelled_mask = (
    master_orders[CANCELLED_COL].notna()
    & (master_orders[CANCELLED_COL].astype(str).str.strip() != "")
)

# Only flip the ones currently marked Completed (case-insensitive)
to_fix = cancelled_mask & (
    master_orders[STATUS_COL].astype(str).str.strip().str.lower() == "completed"
)

master_orders.loc[to_fix, STATUS_COL] = "Cancelled"

print(f"Rows with a Cancelled at value : {cancelled_mask.sum()}")
print(f"Were 'Completed', now fixed to 'Cancelled' : {to_fix.sum()}")


# ---- notebook cell 43 SKIPPED (local parquet save; replaced by Drive save at end) ----


# ---- notebook cell 44 --------------------------------------------------
# ══════════════════════════════════════════════════════════════════════════
# SECTION 19C — Standardize Delivery Time Slot (24-hour, auto-corrects typos)
# Output: "HH:MM to HH:MM"
# Primary source : 'Delivery Time Slot' column (Admin orders)
# Fallback source: 'Tags' column (Shopify orders embed the slot inside Tags)
# Auto-fix       : if end <= start, assumes AM/PM typo and shifts end +12h
# ══════════════════════════════════════════════════════════════════════════

TIME_12H_PATTERN = re.compile(
    r'(\d{1,2}:\d{2})\s*([AP]M)\s*(?:-|–|—|to)\s*(\d{1,2}:\d{2})\s*([AP]M)',
    re.IGNORECASE
)
TIME_24H_PATTERN = re.compile(
    r'(\d{1,2}:\d{2})(?::\d{2})?\s*(?:-|–|—|to)\s*(\d{1,2}:\d{2})(?::\d{2})?',
    re.IGNORECASE
)


def _fix_order(sh, sm, eh, em):
    """If end <= start, shift end by 12 hours (mod 24) — corrects AM/PM typos."""
    if (eh * 60 + em) <= (sh * 60 + sm):
        eh = (eh + 12) % 24
    return sh, sm, eh, em


def extract_slot(value):
    """Extract + normalize a time slot from ANY text to 'HH:MM to HH:MM' (24h).
    Works on both the 'Delivery Time Slot' column and the 'Tags' column.
    Returns None if no valid time-slot pattern is found.
    Handles:
      - '10:00 AM - 01:00 PM'                    →  '10:00 to 13:00'
      - '10:00 AM - 01:00 AM'                    →  '10:00 to 13:00'  (typo fixed)
      - '04:00 PM - 07:00 PM, April 13 2024,'    →  '16:00 to 19:00'  (from Tags)
      - '16:30 to 19:30'                         →  '16:30 to 19:30'  (Admin 24h)
      - '10:00 to 01:00'                         →  '10:00 to 13:00'  (legacy fix)
    """
    if pd.isna(value):
        return None
    s = str(value).strip()
    if not s or s.lower() in ['nan', 'none', '', 'null', 'nat']:
        return None

    # Try 12-hour AM/PM pattern FIRST (Tags + most Shopify formats)
    m12 = TIME_12H_PATTERN.search(s)
    if m12:
        try:
            start_dt = datetime.strptime(f"{m12.group(1)} {m12.group(2).upper()}", "%I:%M %p")
            end_dt   = datetime.strptime(f"{m12.group(3)} {m12.group(4).upper()}", "%I:%M %p")
            sh, sm, eh, em = _fix_order(start_dt.hour, start_dt.minute,
                                        end_dt.hour, end_dt.minute)
            return f"{sh:02d}:{sm:02d} to {eh:02d}:{em:02d}"
        except ValueError:
            pass

    # Then try 24-hour pattern (Admin 'Delivery Time Slot' like '16:30 to 19:30')
    m24 = TIME_24H_PATTERN.search(s)
    if m24:
        try:
            sh, sm = [int(x) for x in m24.group(1).split(':')]
            eh, em = [int(x) for x in m24.group(2).split(':')]
            # sanity: valid clock values only (rejects dates/junk)
            if not (0 <= sh <= 23 and 0 <= eh <= 23 and 0 <= sm <= 59 and 0 <= em <= 59):
                return None
            sh, sm, eh, em = _fix_order(sh, sm, eh, em)
            return f"{sh:02d}:{sm:02d} to {eh:02d}:{em:02d}"
        except ValueError:
            pass

    return None


# ── Capture before-state
before_filled = master_orders['Delivery Time Slot'].notna().sum()
print(f"📊 Before: {before_filled:,} / {len(master_orders):,} rows had a Delivery Time Slot")

# ── Step 1: Normalize whatever is already in the Delivery Time Slot column
master_orders['Delivery Time Slot'] = master_orders['Delivery Time Slot'].apply(extract_slot)
after_primary = master_orders['Delivery Time Slot'].notna().sum()
print(f"   After normalizing existing column: {after_primary:,} filled")

# ── Step 2: For rows STILL blank, fall back to extracting from Tags
needs_fallback = master_orders['Delivery Time Slot'].isna()
if 'Tags' in master_orders.columns:
    master_orders.loc[needs_fallback, 'Delivery Time Slot'] = (
        master_orders.loc[needs_fallback, 'Tags'].apply(extract_slot)
    )
    recovered_mask = needs_fallback & master_orders['Delivery Time Slot'].notna()
    print(f"🔁 Recovered from Tags column: {recovered_mask.sum():,} rows")
else:
    print("⚠️  'Tags' column not found — skipping fallback")
    recovered_mask = pd.Series(False, index=master_orders.index)

# ── After-state
after_filled = master_orders['Delivery Time Slot'].notna().sum()
after_unique = master_orders['Delivery Time Slot'].dropna().nunique()
print(f"\n✅ After: {after_filled:,} / {len(master_orders):,} rows filled "
      f"({after_filled/len(master_orders)*100:.1f}%)")
print(f"   Net new slots recovered: {after_filled - before_filled:,}")
print(f"   Unique slot formats: {after_unique}")

print(f"\n📋 All unique slots now:")
print(master_orders['Delivery Time Slot'].value_counts().to_string())

# ── Sample: rows that were recovered specifically from Tags
print(f"\n🔄 Sample of rows where slot came from Tags ({recovered_mask.sum():,} total):")
if recovered_mask.sum() > 0:
    sample = pd.DataFrame({
        'Name'        : master_orders.loc[recovered_mask, 'Name'].values,
        'sub_channel' : master_orders.loc[recovered_mask, 'sub_channel'].values,
        'Tags (src)'  : master_orders.loc[recovered_mask, 'Tags'].astype(str).str[:45].values,
        'Slot (out)'  : master_orders.loc[recovered_mask, 'Delivery Time Slot'].values,
    }).head(15)
    print(sample.to_string(index=False))


# ---- notebook cell 45 --------------------------------------------------
# ── DIAGNOSTIC: catch any Tags with a time pattern that we FAILED to extract

# Rows where slot is still blank
blank_slot = master_orders['Delivery Time Slot'].isna()

# ...but Tags contains something time-like (HH:MM)
has_timeish = master_orders['Tags'].astype(str).str.contains(r'\d{1,2}:\d{2}', regex=True, na=False)

misses = master_orders[blank_slot & has_timeish]

print(f"🔍 Potential misses (Tags has a time but slot is blank): {len(misses):,}")
if len(misses) > 0:
    print("\n   These Tags formats are NOT being caught — share with me to fix regex:")
    print(misses['Tags'].astype(str).str[:70].value_counts().head(20).to_string())
else:
    print("   ✅ ZERO misses — every Tags slot is being extracted correctly")


# ---- notebook cell 46 --------------------------------------------------
# ══════════════════════════════════════════════════════════════════════════
# SECTION X — Create Delivery Type column (NEW, added alongside existing cols)
# Logic:
#   Shopify rows:  "Express" if Shipping Method contains 'express'
#                  else use Shipping Method value as-is
#   Admin rows:    "Express" if Is Express == 'Yes'
#                  else "Standard"
#   B2B Zoho:      blank (not applicable)
# ══════════════════════════════════════════════════════════════════════════

def determine_delivery_type(row):
    sub = str(row.get('sub_channel', '')).strip()

    # ── Shopify path
    if sub.startswith('Shopify'):
        ship = row.get('Shipping Method')
        if pd.isna(ship):
            return None
        ship_str = str(ship).strip()
        if not ship_str or ship_str.lower() in ['nan', 'none']:
            return None
        if 'express' in ship_str.lower():
            return 'Express'
        return ship_str

    # ── Admin path (B2B_DraftOrder came from admin, same logic)
    if sub.startswith('Admin') or sub == 'B2B_DraftOrder':
        is_express = row.get('Is Express')
        if pd.notna(is_express) and str(is_express).strip().lower() == 'yes':
            return 'Express'
        return 'Standard'

    # ── B2B Zoho — not applicable
    return None


# Apply
master_orders['Delivery Type'] = master_orders.apply(determine_delivery_type, axis=1)

# Drop only the temporary Is Express column (no longer needed)
if 'Is Express' in master_orders.columns:
    master_orders = master_orders.drop(columns=['Is Express'])

# Report
n_filled = master_orders['Delivery Type'].notna().sum()
print(f"✅ Delivery Type column created (Accepts Email Marketing kept as-is)")
print(f"   Filled: {n_filled:,} / {len(master_orders):,} ({n_filled/len(master_orders)*100:.1f}%)")

print(f"\n📋 Delivery Type distribution:")
print(master_orders['Delivery Type'].value_counts().head(20).to_string())

print(f"\n📊 By sub_channel:")
breakdown = (
    master_orders.groupby(['sub_channel', 'Delivery Type'], dropna=False)
    .size()
    .reset_index(name='count')
    .sort_values(['sub_channel', 'count'], ascending=[True, False])
)
print(breakdown.to_string(index=False))


# ---- notebook cell 47 --------------------------------------------------
print("\n" + "="*70)
print("v7 FINAL VERIFICATION")
print("="*70)

# 1. Channel check
print(f"\n✅ Channels present: {sorted(master_orders['channel'].unique().tolist())}")
assert set(master_orders['channel'].unique()) <= {'1P', 'B2B', 'B2C'}, "Unexpected channel found"

# 2. Sub-channel check
print(f"\n✅ Sub-channels (B2B/B2C area):")
b2b_subs = master_orders[master_orders['channel'].isin(['B2B', 'B2C'])]['sub_channel'].unique()
for sub in sorted(b2b_subs):
    count = (master_orders['sub_channel'] == sub).sum()
    print(f"   {sub:25s}: {count:>6,} rows")

# 3. B2B_DraftOrder should NOT be present
draft_count = (master_orders['sub_channel'] == 'B2B_DraftOrder').sum()
print(f"\n✅ B2B_DraftOrder rows: {draft_count} (should be 0)")
assert draft_count == 0, "B2B_DraftOrder still in pipeline!"

# 4. Revenue columns check
print(f"\n✅ Revenue columns:")
for col in ['Lineitem_Revenue', 'Lineitem_Discount', 'Lineitem_Shipping',
            'Tax_Amount', 'Gross_Revenue', 'Net_Revenue']:
    if col in master_orders.columns:
        non_zero = (master_orders[col] != 0).sum()
        total = master_orders[col].sum()
        print(f"   {col:25s}: {non_zero:>6,} non-zero | Total ₹{total:>14,.2f}")

# 5. Math check
print(f"\n✅ Math check: Gross - Tax = Net?")
calculated = master_orders['Gross_Revenue'].sum() - master_orders['Tax_Amount'].sum()
actual = master_orders['Net_Revenue'].sum()
diff = abs(calculated - actual)
print(f"   Gross - Tax: ₹{calculated:,.2f}")
print(f"   Net:         ₹{actual:,.2f}")
print(f"   Difference:  ₹{diff:.2f} {'✅ PASS' if diff < 10 else '❌ FAIL'}")

# 6. Channel-wise breakdown
print(f"\n📊 Final breakdown by channel:")
summary = master_orders.groupby('channel').agg(
    rows=('Net_Revenue', 'count'),
    gross=('Gross_Revenue', 'sum'),
    tax=('Tax_Amount', 'sum'),
    net=('Net_Revenue', 'sum'),
).round(0)
print("pushed successfully")


# ---- notebook cell 48 --------------------------------------------------
required_cols = ['lifetime_order_no', 'segment_dynamic',
                 'total_lifetime_orders', 'segment_lifetime',
                 'transition_tag', 'transition_days',
                 'Net_Revenue', 'Gross_Revenue', 'Tax_Amount',     # v7 NEW
                 'Lineitem_Revenue', 'Lineitem_Discount', 'Lineitem_Shipping',
                 'city', 'state', 'zip']
missing = [c for c in required_cols if c not in master_orders.columns]
if missing:
    raise Exception(f"❌ Missing required columns: {missing}")

gc = pygsheets.authorize(service_account_file="dgf-analytics-429368876a21.json")
sh = gc.open("Analytics Tracker | DGF")
wks = sh.worksheet_by_title("Master_orders")

df_push = master_orders.copy().astype(str).replace('None', '').replace('nan', '').replace('NaT', '')

wks.resize(rows=len(df_push) + 10, cols=len(df_push.columns) + 5)
wks.clear(start='A1')
wks.set_dataframe(df_push, start='A1', copy_index=False, copy_head=True)

print(f"✅ {df_push.shape[0]:,} rows × {df_push.shape[1]} cols pushed to Sheets")
print(f"   v7 columns included: Gross_Revenue, Tax_Amount, Net_Revenue (redefined)")



# ---- notebook cells 49 (freq segment, commented) & 50 (Saksham sheet) SKIPPED per request ----

# ══════════════════════════════════════════════════════════════════════════
# SAVE master_orders.parquet → GOOGLE DRIVE  (replaces notebook cell 43)
# Saved AFTER all transformations so logistics/catalogue pipelines get
# the complete dataset (incl. Delivery Time Slot + Delivery Type).
# ══════════════════════════════════════════════════════════════════════════
print("\n" + "="*70)
print("💾 SAVING master_orders.parquet TO GOOGLE DRIVE")
print("="*70)

PARQUET_PATH = os.path.join(OUTPUT_DIR, "master_orders.parquet")
master_orders.to_parquet(PARQUET_PATH, index=False, engine="pyarrow")
_mb = os.path.getsize(PARQUET_PATH) / (1024 * 1024)
print(f"   ✅ Written locally: {PARQUET_PATH} ({_mb:.2f} MB)")

drive_upload_to_folder(PARQUET_PATH, "master_orders.parquet", DRIVE_FOLDER_ID)

print("\n" + "="*70)
print("✅ PIPELINE COMPLETE")
print("="*70)
print(f"   Rows: {len(master_orders):,}  |  Columns: {master_orders.shape[1]}")
print(f"   Net Revenue: ₹{pd.to_numeric(master_orders['Net_Revenue'], errors='coerce').sum():,.0f}")
print("\n   Google Drive (Analytics Pipeline Files):")
print("     • master_orders.parquet   (updated)")
print("     • B2B_master.pkl          (updated)")
print("     • B2B_sync_state.json     (updated)")
print("   Google Sheets:")
print("     • COGS workbook           → COGS_Daily_Full / _Summary / _B2B_Only")
print("     • Analytics Tracker | DGF → Master_orders")
print("="*70)
