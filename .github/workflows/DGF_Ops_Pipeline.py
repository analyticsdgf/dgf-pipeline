#!/usr/bin/env python
# coding: utf-8
# ══════════════════════════════════════════════════════════════════════════
# DGF OPS PIPELINE — GITHUB ACTIONS AUTOMATION (Pipeline #2)
# ══════════════════════════════════════════════════════════════════════════
# Runs AFTER the main DGF Pipeline (which refreshes master_orders.parquet
# on Google Drive). Three stages, in dependency order:
#
#   STAGE 1: PRODUCT CATALOGUE  (PostgreSQL → Product_Catalogue sheet tab)
#            Runs first because Stage 2 reads product weights from this tab.
#   STAGE 2: LOGISTICS COST DOD (master_orders.parquet from Drive
#            + Product_Catalogue weights + Del_Boy_Record driver costs
#            → Logistics_DOD / Logistics_Detail / Logistics_Order tabs)
#   STAGE 3: MARKETING COMBINER (Marketing_Google_Ads + Marketing_Meta_Ads
#            + Marketing_WATI tabs → Marketing_Cost tab)
#
# Each stage is isolated in try/except: one failing stage does NOT stop
# the others. Exit code 1 at the end if ANY stage failed (so GitHub shows
# the run red and you get notified).
# ══════════════════════════════════════════════════════════════════════════

remove this code to make this code work

import os, re, io, sys, json, math, base64, traceback
import numpy as np
import pandas as pd
from datetime import datetime
from urllib.parse import quote_plus
from sqlalchemy import create_engine, text
import psycopg2

import pygsheets
import gspread
from gspread.utils import rowcol_to_a1
from gspread_dataframe import set_with_dataframe, get_as_dataframe
from google.oauth2.service_account import Credentials
from googleapiclient.discovery import build
from googleapiclient.http import MediaIoBaseDownload, MediaFileUpload

print("✅ Imports loaded")

# ══════════════════════════════════════════════════════════════════════════
# 0. ENVIRONMENT + CREDENTIALS (same secrets as Pipeline #1 — same repo)
# ══════════════════════════════════════════════════════════════════════════
OUTPUT_DIR = "/tmp" if os.name != "nt" else r"D:\downloads"
os.makedirs(OUTPUT_DIR, exist_ok=True)
os.chdir(OUTPUT_DIR)

DB_HOST     = os.environ.get("DB_HOST",     "dgfdb.clsykici051f.ap-south-1.rds.amazonaws.com")
DB_PORT     = os.environ.get("DB_PORT",     "5432")
DB_NAME     = os.environ.get("DB_NAME",     "dgfdb")
DB_USER     = os.environ.get("DB_USER",     "readonly_user")
DB_PASSWORD = os.environ.get("DB_PASSWORD", "Gurugram@2026")

SERVICE_ACCOUNT_FILE = "dgf-analytics-429368876a21.json"
DRIVE_FOLDER_ID      = os.environ.get("DRIVE_ANALYTICS_FOLDER_ID", "")

# Write the service-account JSON from the BASE64 secret (same as Pipeline #1)
if not os.path.exists(SERVICE_ACCOUNT_FILE):
    sa_b64 = os.environ.get("GOOGLE_SERVICE_ACCOUNT_JSON_BASE64", "")
    if sa_b64:
        try:
            with open(SERVICE_ACCOUNT_FILE, "w") as f:
                f.write(base64.b64decode(sa_b64).decode("utf-8"))
            print("✅ Service-account JSON written from base64 secret")
        except Exception as e:
            print(f"❌ Could not decode GOOGLE_SERVICE_ACCOUNT_JSON_BASE64: {e}")
    else:
        print("⚠️  GOOGLE_SERVICE_ACCOUNT_JSON_BASE64 secret not set")

print(f"✅ Working directory: {OUTPUT_DIR}")
print(f"✅ Drive folder set : {'yes' if DRIVE_FOLDER_ID else 'NO'}")

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
            # UPDATE keeps the file owned by YOU (uses your quota) — always works
            service.files().update(fileId=existing, media_body=media).execute()
            print(f"   ✅ Updated on Drive: {filename}")
            return True
        # CREATE makes the service account the owner — personal My Drive
        # gives service accounts ZERO storage quota, so this can 403.
        try:
            service.files().create(
                body={"name": filename, "parents": [folder_id]},
                media_body=media,
            ).execute()
            print(f"   ✅ Uploaded to Drive: {filename}")
            return True
        except Exception as ce:
            if "storageQuota" in str(ce) or "quota" in str(ce).lower():
                print(f"   ⚠️  Cannot CREATE {filename} (service accounts have no storage quota).")
                print(f"       ONE-TIME FIX: manually upload any small placeholder file named")
                print(f"       exactly '{filename}' into the Drive folder from YOUR account.")
                print(f"       After that, this pipeline will UPDATE it successfully every run.")
            else:
                print(f"   ⚠️  Upload failed for {filename}: {ce}")
            return False
    except Exception as e:
        print(f"   ⚠️  Upload failed for {filename}: {e}")
        return False


# ── Stage status tracker ────────────────────────────────────────────────
STAGE_STATUS = {"catalogue": None, "logistics": None, "marketing": None}


# ══════════════════════════════════════════════════════════════════════════
# STAGE 1 — PRODUCT CATALOGUE  (PostgreSQL → Product_Catalogue tab)
# ══════════════════════════════════════════════════════════════════════════
print("\n" + "="*70)
print("🐟 STAGE 1: PRODUCT CATALOGUE")
print("="*70)

DB = dict(host=DB_HOST, port=int(DB_PORT), database=DB_NAME,
          user=DB_USER, password=DB_PASSWORD)

SHEET_ID = "138uZD2oy5YplGg7DOox8RVyXM2II-Pz6UPcGSOWeHQ4"
TAB_NAME = "Product_Catalogue"
ADMIN_URL_TEMPLATE = "https://admin.damgoodfish.com/items/new?id={product_id}"

MANUAL_COLS = [
    "habitat_final", "cut_final", "prep_final", "species_final",
    "weight_band", "cuisine_tags", "cooking_method",
    "alternate_names", "notes",
]

# NOTE: The original notebook's SQL cell was truncated in the .ipynb file
# (the hsn join + fetch function were missing). Instead of guessing the hsn
# table/column names, we now ASK the database itself via information_schema
# (PostgreSQL's built-in dictionary of every table & column). Discovery:
#   Case A: products table has hsn/gst columns directly  → select them
#   Case B: a separate %hsn% table exists                → build the join
#   Case C: nothing found                                → blank hsn/gst
_SQL_BODY = """
SELECT
    p.product_id, p.sku, p.title, p.slug, p.status,
    p.compare_price AS mrp, p.price AS sp, p.charge_tax,
    {HSN_COLS}
    p.quantity, u.unit, p.pieces, p.serve_person AS serves, p.display_quantity,
    p.suitable_for, p.fresh, p.chemical_free, p.natural, p.no_antibiotic,
    p.sell_out_of_stock, p.show_badge, p.force_feed,
    p.description, p.ingredients, p.self_life AS shelf_life,
    p.seo_title, p.seo_description,
    (SELECT string_agg(DISTINCT c.category_name, ', ' ORDER BY c.category_name)
       FROM products_category pc
       JOIN categories c ON c.category_id = pc.category_id
      WHERE pc.product_id = p.product_id)  AS categories,
    (SELECT string_agg(DISTINCT col.title, ', ' ORDER BY col.title)
       FROM products_collection pcol
       JOIN collections col ON col.collection_id = pcol.collection_id
      WHERE pcol.product_id = p.product_id) AS collections,
    p.created_date, p.updated_date
FROM products p
LEFT JOIN unit u ON u.unit_id = p.unit_id
{HSN_JOIN}
"""

_CAT_CONN_STR = (
    f"postgresql+psycopg2://{DB_USER}:{quote_plus(DB_PASSWORD)}"
    f"@{DB_HOST}:{DB_PORT}/{DB_NAME}?sslmode=require"
)


def _cols_of(engine, table):
    q = text("SELECT column_name FROM information_schema.columns WHERE table_name = :t")
    return pd.read_sql(q, engine, params={"t": table})["column_name"].tolist()


def _discover_hsn(engine):
    """Return (HSN_COLS, HSN_JOIN) by inspecting the live schema."""
    try:
        prod_cols = _cols_of(engine, "products")

        # Case A: hsn/gst columns live directly on products
        hsn_direct = next((c for c in prod_cols
                           if "hsn" in c.lower() and not c.lower().endswith("id")), None)
        gst_direct = next((c for c in prod_cols if "gst" in c.lower()), None)
        if hsn_direct:
            cols = f"p.{hsn_direct} AS hsn_code, "
            cols += f"p.{gst_direct} AS gst_pct," if gst_direct else "NULL AS gst_pct,"
            print(f"   🔎 hsn discovery: found directly on products ({hsn_direct}, {gst_direct})")
            return cols, ""

        # Case B: separate hsn-like table
        tq = text("""SELECT table_name FROM information_schema.tables
                     WHERE table_schema='public' AND table_name ILIKE :pat""")
        hsn_tables = pd.read_sql(tq, engine, params={"pat": "%hsn%"})["table_name"].tolist()
        p_key = next((c for c in prod_cols if "hsn" in c.lower() and "id" in c.lower()), None)
        for t in hsn_tables:
            tcols = _cols_of(engine, t)
            t_key = (next((c for c in tcols if "hsn" in c.lower() and "id" in c.lower()), None)
                     or ("id" if "id" in tcols else None))
            if not (p_key and t_key):
                continue
            hsn_c = next((c for c in tcols if "code" in c.lower()), None)
            gst_c = next((c for c in tcols if "gst" in c.lower() or "percent" in c.lower()), None)
            cols = (f"h.{hsn_c} AS hsn_code, " if hsn_c else "NULL AS hsn_code, ")
            cols += (f"h.{gst_c} AS gst_pct," if gst_c else "NULL AS gst_pct,")
            join = f"LEFT JOIN {t} h ON h.{t_key} = p.{p_key}"
            print(f"   🔎 hsn discovery: table '{t}' via {t_key}={p_key} (code={hsn_c}, gst={gst_c})")
            return cols, join

        print("   🔎 hsn discovery: nothing found — hsn_code/gst_pct will be blank")
        return "", ""
    except Exception as e:
        print(f"   ⚠️  hsn discovery failed ({str(e)[:80]}) — proceeding without hsn")
        return "", ""


def fetch_from_db():
    eng = create_engine(_CAT_CONN_STR)
    try:
        hsn_cols, hsn_join = _discover_hsn(eng)
        sql = _SQL_BODY.format(HSN_COLS=hsn_cols, HSN_JOIN=hsn_join)
        try:
            df = pd.read_sql(text(sql), eng)
        except Exception as e:
            print(f"   ⚠️  discovered-hsn query failed ({str(e)[:80]}) — retrying without hsn")
            df = pd.read_sql(text(_SQL_BODY.format(HSN_COLS="", HSN_JOIN="")), eng)
        if "hsn_code" not in df.columns: df["hsn_code"] = None
        if "gst_pct"  not in df.columns: df["gst_pct"]  = None
    finally:
        eng.dispose()
    # Decimal → float (PostgreSQL NUMERIC arrives as Decimal objects)
    for c in ["mrp", "sp", "quantity", "gst_pct"]:
        if c in df.columns:
            df[c] = pd.to_numeric(df[c], errors="coerce")
    return df


EXOTIC_SPECIES = [
    "atlantic salmon", "norwegian salmon", "salmon",
    "squid", "squids", "calamari", "octopus",
]
MARINE_SALMON_GUARD = ["rawaas", "indian salmon", "gurjali", "vazhmeen"]

SPECIES_HABITAT_MAP = {
    "rohu": "Freshwater", "rui": "Freshwater", "catla": "Freshwater",
    "hilsa": "Freshwater", "ilish": "Freshwater", "tilapia": "Freshwater",
    "basa": "Freshwater", "pangasius": "Freshwater", "singhara": "Freshwater",
    "aar": "Freshwater", "bacha": "Freshwater", "bhola": "Freshwater",
    "tengra": "Freshwater", "pabda": "Freshwater", "parshe": "Freshwater",
    "murrel": "Freshwater", "raiya": "Freshwater", "roopchand": "Freshwater",
    "river pomfret": "Freshwater", "himalayan trout": "Freshwater",
    "trout": "Freshwater", "singi": "Freshwater", "singhi": "Freshwater",
    "river sole": "Freshwater", "river sol": "Freshwater",
    "sole": "Freshwater", "sol": "Freshwater",
    "boal": "Freshwater", "wallago": "Freshwater",
    "clams": "Marine",
    "seawater sole": "Marine", "surmai": "Marine", "seer": "Marine",
    "white pomfret": "Marine", "black pomfret": "Marine", "pomfret": "Marine",
    "mackerel": "Marine", "bangda": "Marine", "sardine": "Marine",
    "sea bass": "Marine", "seabass": "Marine", "bhetki": "Marine",
    "reef cod": "Marine", "bombay duck": "Marine", "mahi mahi": "Marine",
    "pink perch": "Marine", "red snapper": "Marine", "snapper": "Marine",
    "leather jacket": "Marine", "choora": "Marine", "tuna": "Marine",
    "rawaas": "Marine", "indian salmon": "Marine",
    "gurjali": "Marine", "vazhmeen": "Marine",
    "tiger prawns": "Marine", "prawns": "Marine", "scampi": "Marine",
    "blue crab": "Marine", "crab": "Marine", "green mussels": "Marine",
    "mussels": "Marine", "oyster": "Marine", "soft shell crab": "Marine",
    "atlantic salmon": "Exotic", "salmon": "Exotic",
    "squids": "Exotic", "squid": "Exotic", "octopus": "Exotic",
    "calamari": "Exotic",
}

MARINADE_KEYWORDS = [
    "achari", "tandoori", "amritsari", "tikka masala", "tikka",
    "chilli garlic", "chili garlic", "kali mirch", "smoky charcoal", "smoky",
    "spicy grill", "gochujang", "garden mint", "marinated",
    "kebab", "patty", "burger", "fish finger", "fish bites", "crispy fish",
    "cajun",
]

CUT_PATTERNS = [
    (r"\bboneless\s+cube",                "Boneless Cubes"),
    (r"\bboneless\s+fillet",              "Boneless Fillets"),
    (r"\bboneless\s+full\s+clean",        "Boneless Fillets"),
    (r"\bround\s+(?:bengali\s+)?cut",     "Round Cut"),
    (r"\bcurry\s+cut",                    "Curry Cut"),
    (r"\bfry\s+cut",                      "Fry Cut"),
    (r"\bfinger\s+cut",                   "Finger Cut"),
    (r"\bfish\s+finger",                  "Ready-to-Cook"),
    (r"\bhead\s+(?:cut|cleaned|&)",       "Head Cut"),
    (r"\bwhole\s+clean",                  "Whole & Cleaned"),
    (r"\bwhole\b",                        "Whole & Cleaned"),
    (r"\bwhoel\b",                        "Whole & Cleaned"),
    (r"\bg&c\b",                          "Whole & Cleaned"),
    (r"\bpeeled.*deveined|\bdeveined",    "Peeled & Deveined"),
    (r"\brings\s+cleaned",                "Rings"),
    (r"\btubes\s+cleaned",                "Tubes"),
    (r"\biqf\b",                          "Whole & Cleaned"),
    (r"\bsoft\s+shell",                   "Whole & Cleaned"),
    (r"\bbaby\s+octopus",                 "Whole & Cleaned"),
    (r"\bgreen\s+mussels",               "Whole & Cleaned"),
    (r"\bwith\s+shell",                   "Whole & Cleaned"),
    (r"\bkebab\b|\bburger\b|\bpatty\b|fish\s+bites", "Ready-to-Cook"),
    (r"\bfillet",                         "Fillets"),
    (r"\bcube",                           "Cubes"),
    (r"\bsteak",                          "Steaks"),
]


def derive_habitat(row):
    title      = str(row.get("title")      or "").lower()
    categories = str(row.get("categories") or "").lower()
    sku        = str(row.get("sku")        or "").upper()
    if re.search(r"\bb2b\b", title):
        return "B2B Products"
    if not any(g in title for g in MARINE_SALMON_GUARD):
        for sp in EXOTIC_SPECIES:
            if re.search(rf"\b{re.escape(sp)}\b", title):
                return "Exotic"
    if "freshwater" in categories:
        return "Freshwater"
    if "marinewater" in categories or "marine water" in categories:
        return "Marine"
    if "exotic" in categories:
        return "Exotic"
    for sp in sorted(SPECIES_HABITAT_MAP, key=len, reverse=True):
        if re.search(rf"\b{re.escape(sp)}\b", title):
            return SPECIES_HABITAT_MAP[sp]
    pre = sku.split("-")[0] if sku else ""
    if pre in ("RF", "RH"): return "Freshwater"
    if pre == "RM":         return "Marine"
    if pre == "RS":         return "Exotic"
    return "Other"


def derive_prep(row):
    title      = str(row.get("title")      or "").lower()
    categories = str(row.get("categories") or "").lower()
    sku        = str(row.get("sku")        or "").upper()
    for kw in MARINADE_KEYWORDS:
        if re.search(rf"\b{re.escape(kw)}\b", title):
            return "Marinated"
    if "marinated" in categories:
        return "Marinated"
    parts = sku.split("-")
    prefix = parts[0] if parts else ""
    if prefix == "MR":
        return "Marinated"
    if prefix == "BL" and len(parts) >= 2:
        if parts[1] == "MR": return "Marinated"
        if parts[1] == "RW": return "Raw"
    return "Raw"


def derive_cut(title):
    if not isinstance(title, str): return "Other"
    t = title.lower()
    for pat, label in CUT_PATTERNS:
        if re.search(pat, t):
            return label
    return "Other"


def derive_species(title):
    if not isinstance(title, str): return ""
    t = title.lower()
    for sp in sorted(SPECIES_HABITAT_MAP, key=len, reverse=True):
        if re.search(rf"\b{re.escape(sp)}\b", t):
            return sp.title()
    return ""


def derive_weight_grams(row):
    q, u = row.get("quantity"), str(row.get("unit") or "").lower()
    if pd.notna(q):
        try:
            q = float(q)
            if u in ("g", "gm", "gram", "grams"): return int(q)
            if u in ("kg", "kgs"):                return int(q * 1000)
        except (ValueError, TypeError):
            pass
    title = str(row.get("title") or "")
    m = re.search(r"(\d+(?:\.\d+)?)\s*(g|kg)\b", title, re.I)
    if m:
        val, unit = float(m.group(1)), m.group(2).lower()
        return int(val * 1000) if unit == "kg" else int(val)
    return None


def add_derived_columns(df):
    df["habitat_auto"] = df.apply(derive_habitat, axis=1)
    df["prep_auto"]    = df.apply(derive_prep,    axis=1)
    df["cut_auto"]     = df["title"].apply(derive_cut)
    df["species_auto"] = df["title"].apply(derive_species)
    df["net_weight_g"] = df.apply(derive_weight_grams, axis=1)
    df["discount_pct"] = (
        ((df["mrp"] - df["sp"]) / df["mrp"] * 100)
        .round(1).where(df["mrp"].notna() & (df["mrp"] > 0))
    )
    df["last_synced"]   = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    df["source_status"] = df["status"].apply(lambda x: "ACTIVE" if x == True else "INACTIVE")

    def _build_link(row):
        if pd.isna(row.get("product_id")):
            return ""
        url = ADMIN_URL_TEMPLATE.format(
            product_id=row.get("product_id"), sku=row.get("sku"),
        )
        return f'=HYPERLINK("{url}", "Open in admin")'
    df["admin_link"] = df.apply(_build_link, axis=1)
    return df


def clean_and_deduplicate(df):
    before = len(df)
    blank_mask = df['sku'].isna() | (df['sku'].astype(str).str.strip() == '')
    if blank_mask.any():
        print(f"  Removed {blank_mask.sum()} row(s) with blank SKU")
    df = df[~blank_mask].copy()
    df = df.sort_values(['sku', 'status', 'product_id'],
                        ascending=[True, False, False])
    dup_mask = df.duplicated(subset=['sku'], keep=False)
    if dup_mask.any():
        duplicates = df[dup_mask]
        print(f"  ⚠️  {dup_mask.sum()} rows share {duplicates['sku'].nunique()} duplicate SKU(s) — keeping active/newer")
    df_clean = df.drop_duplicates(subset=['sku'], keep='first')
    print(f"  ✓ Cleaned: {before} → {len(df_clean)} rows")
    return df_clean


def get_worksheet():
    gc = gspread.service_account(filename=SERVICE_ACCOUNT_FILE)
    sh = gc.open_by_key(SHEET_ID)
    try:
        ws = sh.worksheet(TAB_NAME)
    except gspread.WorksheetNotFound:
        ws = sh.add_worksheet(title=TAB_NAME, rows=500, cols=50)
        print(f"Created new tab: {TAB_NAME}")
    return ws


def _write_with_formulas(ws, df_ordered):
    set_with_dataframe(ws, df_ordered)
    if "admin_link" in df_ordered.columns:
        col_idx = list(df_ordered.columns).index("admin_link") + 1
        top = rowcol_to_a1(2, col_idx)
        bot = rowcol_to_a1(len(df_ordered) + 1, col_idx)
        range_name = f"{top}:{bot}"
        values = [
            [v if (pd.notna(v) and str(v).strip()) else ""]
            for v in df_ordered["admin_link"].tolist()
        ]
        ws.update(values=values, range_name=range_name,
                  value_input_option="USER_ENTERED")


def _order_columns(df):
    order = [
        "product_id", "sku", "admin_link", "title", "slug", "status", "source_status",
        "mrp", "sp", "discount_pct", "charge_tax", "hsn_code", "gst_pct",
        "quantity", "unit", "net_weight_g", "pieces", "serves", "display_quantity",
        "habitat_auto", "cut_auto", "prep_auto", "species_auto",
        "habitat_final", "cut_final", "prep_final", "species_final",
        "weight_band", "cuisine_tags", "cooking_method", "alternate_names", "notes",
        "suitable_for", "fresh", "chemical_free", "natural", "no_antibiotic",
        "categories", "collections",
        "description", "ingredients", "shelf_life", "seo_title", "seo_description",
        "sell_out_of_stock", "show_badge", "force_feed",
        "created_date", "updated_date", "last_synced",
    ]
    existing = [c for c in order if c in df.columns]
    extras   = [c for c in df.columns if c not in existing]
    return df[existing + extras]


def upsert(df_new):
    ws = get_worksheet()
    for col in MANUAL_COLS:
        if col not in df_new.columns:
            df_new[col] = ""
    df_old = get_as_dataframe(ws, evaluate_formulas=False).dropna(how="all")
    if df_old.empty or "sku" not in df_old.columns:
        ws.clear()
        _write_with_formulas(ws, _order_columns(df_new))
        print(f"First run: wrote {len(df_new)} rows")
        return
    df_old["sku"] = df_old["sku"].astype(str)
    df_new["sku"] = df_new["sku"].astype(str)
    manual_existing = [c for c in MANUAL_COLS if c in df_old.columns]
    if manual_existing:
        df_new = df_new.merge(
            df_old[["sku"] + manual_existing],
            on="sku", how="left", suffixes=("", "_old"),
        )
        for col in manual_existing:
            old_col = f"{col}_old"
            if old_col in df_new.columns:
                df_new[col] = df_new[old_col].where(
                    df_new[old_col].notna() & (df_new[old_col].astype(str) != ""),
                    df_new[col],
                )
                df_new.drop(columns=[old_col], inplace=True)
    missing_skus = set(df_old["sku"]) - set(df_new["sku"])
    if missing_skus:
        ghost = df_old[df_old["sku"].isin(missing_skus)].copy()
        ghost["source_status"] = "DELETED"
        ghost["last_synced"]   = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        df_new = pd.concat([df_new, ghost], ignore_index=True)
        print(f"Preserved {len(missing_skus)} deleted SKUs as ghost rows")
    ws.clear()
    _write_with_formulas(ws, _order_columns(df_new))
    print(f"Upserted {len(df_new)} rows")


try:
    print("→ Fetching ALL products from PostgreSQL...")
    _cat_df = fetch_from_db()
    print(f"→ {len(_cat_df)} products fetched")
    print("→ Deriving taxonomy (multi-signal logic)...")
    _cat_df = add_derived_columns(_cat_df)
    print("→ Cleaning & deduplicating...")
    _cat_df = clean_and_deduplicate(_cat_df)
    print(f"Final: {len(_cat_df)} unique SKUs")
    print("  Habitat:", {k: int(v) for k, v in _cat_df["habitat_auto"].value_counts().items()})
    print("  Status: ", {k: int(v) for k, v in _cat_df["source_status"].value_counts().items()})
    print(f"→ Pushing to Google Sheet ({TAB_NAME})...")
    upsert(_cat_df)
    print("✓ STAGE 1 DONE.")
    STAGE_STATUS["catalogue"] = "OK"
except Exception as e:
    STAGE_STATUS["catalogue"] = f"FAILED: {e}"
    print(f"\n❌ STAGE 1 (Catalogue) FAILED: {e}")
    traceback.print_exc()


# ══════════════════════════════════════════════════════════════════════════
# STAGE 2 — LOGISTICS COST DOD
# Input : master_orders.parquet (Google Drive — refreshed by Pipeline #1)
#         Product_Catalogue tab (weights — refreshed by STAGE 1 above)
#         Del_Boy_Record tab (driver cost)
# Output: Logistics_DOD / Logistics_Detail / Logistics_Order tabs
#         + logistics parquet snapshots to Drive (update-only)
# ══════════════════════════════════════════════════════════════════════════
print("\n" + "="*70)
print("🚚 STAGE 2: LOGISTICS COST DOD")
print("="*70)

try:
    # ── CONFIG (from notebook cell 0) ────────────────────────────────────
    CAT_TAB      = "Product_Catalogue"
    TRACKER_NAME = "Analytics Tracker | DGF"

    DRIVER_SHEET_ID = "15mcW54sk8duyDb3J80SIjx1HLwaJtpYr7UnQCX1eD3g"
    DRIVER_TAB      = "Del_Boy_Record"
    INCLUDE_DRIVER  = True

    USE_DB_FALLBACK = True
    DB_CONN_STR = (
        f"postgresql+psycopg2://{DB_USER}:{quote_plus(DB_PASSWORD)}"
        f"@{DB_HOST}:{DB_PORT}/{DB_NAME}?sslmode=require"
    )

    CHANNELS_FOR_DOD = ["1P"]
    STATUSES_FOR_DOD = ["Completed"]
    DATE_BASIS       = "Created at"

    SMALL_RATE, LARGE_RATE, SMALL_MAX_G = 3.5, 4.5, 300
    MASTER_PACK_CAPACITY_G = 1000.0
    MASTER_PACK_COST       = 6.00
    LABEL_COST             = 1.75
    BILL_COST_PER_ORDER    = 2 * 2.85

    def inside_pack_rate(weight_g):
        if pd.isna(weight_g): return LARGE_RATE
        return SMALL_RATE if float(weight_g) <= SMALL_MAX_G else LARGE_RATE

    def pack_band(weight_g):
        if pd.isna(weight_g): return "Unknown (no weight)"
        return f"Small (≤{SMALL_MAX_G}g)" if float(weight_g) <= SMALL_MAX_G else f"Large (>{SMALL_MAX_G}g)"

    def norm_sku(s):
        if pd.isna(s): return None
        s = str(s).strip().upper()
        s = re.sub(r"[\s_]+", "-", s)
        s = re.sub(r"-+", "-", s).strip("-")
        return s if s and s != "NAN" else None

    print(f"Inside packing: ≤{SMALL_MAX_G}g=₹{SMALL_RATE} | >{SMALL_MAX_G}g=₹{LARGE_RATE}")
    print(f"Master pack: ₹{MASTER_PACK_COST}/box for ≤{int(MASTER_PACK_CAPACITY_G)}g")
    print(f"Label ₹{LABEL_COST}/item | Bill ₹{BILL_COST_PER_ORDER}/order")

    # ── 1: LOAD master_orders — FROM GOOGLE DRIVE (was local D:\downloads)
    MASTER_PARQUET = os.path.join(OUTPUT_DIR, "master_orders.parquet")
    print("\n📥 Downloading master_orders.parquet from Google Drive...")
    if not DRIVE_FOLDER_ID:
        raise RuntimeError("DRIVE_ANALYTICS_FOLDER_ID not set — cannot fetch master_orders.parquet")
    _fid = drive_find_in_folder("master_orders.parquet", DRIVE_FOLDER_ID)
    if not _fid:
        raise RuntimeError(
            "master_orders.parquet is NOT INSIDE the 'Analytics Pipeline Files' folder. "
            "Uploading to Drive Home puts it in My Drive root — that is not enough. "
            "In Drive: right-click the file → Organise → Move → into the folder. Then re-run."
        )
    drive_download_by_id(_fid, MASTER_PARQUET)
    if os.path.getsize(MASTER_PARQUET) < 1024:
        raise RuntimeError(
            "master_orders.parquet in the folder is still an EMPTY placeholder. "
            "Run the main 'DGF Pipeline Scheduler' once — it fills this file with real "
            "data (~5 MB) — then this pipeline will work (it also auto-runs after it)."
        )

    mo = pd.read_parquet(MASTER_PARQUET)
    mo["Created at"]        = pd.to_datetime(mo["Created at"], errors="coerce")
    mo["Lineitem quantity"] = pd.to_numeric(mo["Lineitem quantity"], errors="coerce").fillna(0)
    mo["sku_raw"]           = mo["Lineitem sku"].astype(str)
    mo["sku_key"]           = mo["sku_raw"].apply(norm_sku)
    print(f"Loaded {len(mo):,} line rows | {mo['Name'].nunique():,} orders")

    # ── 2: SKU → weight map (catalogue first, DB fallback) ───────────────
    gc_l = gspread.service_account(filename=SERVICE_ACCOUNT_FILE)
    ws_l = gc_l.open_by_key(SHEET_ID).worksheet(CAT_TAB)
    cat_raw = get_as_dataframe(ws_l, evaluate_formulas=False).dropna(how="all")
    print(f"Catalogue rows: {len(cat_raw):,}")

    def norm_col(c):
        s = str(c).strip().lower()
        s = re.sub(r"[^a-z0-9_]+", "_", s)
        return s.strip("_")
    cat_raw.columns = [norm_col(c) for c in cat_raw.columns]

    sku_candidates = [c for c in cat_raw.columns if "sku" in c]
    sku_col = sku_candidates[0] if sku_candidates else "sku"

    weight_cols_priority = ["net_weight_g","net_weight","weight_g","weight","quantity","qty"]
    unit_col = next((c for c in ["unit","units","weight_unit","uom"] if c in cat_raw.columns), None)
    found_weight_cols = [c for c in weight_cols_priority if c in cat_raw.columns]
    print(f"Weight columns found (priority order): {found_weight_cols}")

    cat = cat_raw.copy()
    cat["sku_key"] = cat[sku_col].apply(norm_sku)
    cat = cat.dropna(subset=["sku_key"]).drop_duplicates(subset="sku_key", keep="first")

    def resolve_weight(row):
        for wc in found_weight_cols:
            raw = row.get(wc)
            if pd.isna(raw): continue
            try:
                w = float(str(raw).strip().replace(",",""))
            except: continue
            if w <= 0: continue
            if unit_col:
                u = str(row.get(unit_col,"")).strip().lower()
                if u in ("kg","kgs","kilogram","kilograms"):
                    w = w * 1000
            if wc in ("quantity","qty") and w < 10:
                w = w * 1000
            return w
        return None

    cat["resolved_weight_g"] = cat.apply(resolve_weight, axis=1)
    SKU_WEIGHT = {}
    for k, w in zip(cat["sku_key"], cat["resolved_weight_g"]):
        if pd.notna(w) and w > 0:
            SKU_WEIGHT[k] = w
    print(f"✅ Catalogue map: {len(SKU_WEIGHT):,} SKUs with valid weight")

    sold_skus = set(mo["sku_key"].dropna().unique())
    missing = sold_skus - set(SKU_WEIGHT.keys())
    print(f"Sold SKUs without weight after catalogue lookup: {len(missing)}")

    if USE_DB_FALLBACK and len(missing):
        try:
            eng = create_engine(DB_CONN_STR)
            with eng.connect() as conn:
                wcols = pd.read_sql(text("""
                    SELECT column_name FROM information_schema.columns
                    WHERE table_name='products'
                      AND (column_name ILIKE '%weight%' OR column_name ILIKE '%quantity%')
                """), conn)
            col_list = ", ".join(wcols["column_name"].tolist())
            db_prod = pd.read_sql(f"SELECT sku, {col_list} FROM products", eng)
            db_prod["sku_key"] = db_prod["sku"].apply(norm_sku)
            added = 0
            for _, r in db_prod.iterrows():
                k = r["sku_key"]
                if not k or k in SKU_WEIGHT: continue
                for wc in wcols["column_name"]:
                    v = r.get(wc)
                    if pd.isna(v): continue
                    try: w = float(str(v).strip().replace(",",""))
                    except: continue
                    if w <= 0: continue
                    if w < 10: w *= 1000
                    SKU_WEIGHT[k] = w; added += 1; break
            eng.dispose()
            print(f"   ✅ DB fallback added {added} SKUs")
        except Exception as e:
            print(f"   ⚠️  DB fallback failed: {e}")

    mo["weight_g"] = mo["sku_key"].map(SKU_WEIGHT)
    cov = mo["weight_g"].notna().mean() * 100
    print(f"🎯 FINAL weight coverage on master_orders rows: {cov:.1f}%")
    still_missing = mo[mo["weight_g"].isna()]["sku_raw"].value_counts()
    if len(still_missing):
        print(f"⚠️  {len(still_missing)} SKUs still unmapped (top 10):")
        print(still_missing.head(10).to_string())

    # ── 3: LINE-LEVEL costs ───────────────────────────────────────────────
    mo["pack_band"]       = mo["weight_g"].apply(pack_band)
    mo["inside_packing"]  = mo["weight_g"].apply(inside_pack_rate) * mo["Lineitem quantity"]
    mo["labels"]          = mo["Lineitem quantity"] * LABEL_COST
    mo["line_weight_g"]   = mo["weight_g"].fillna(0) * mo["Lineitem quantity"]
    mo["inside_packing"]  = mo["inside_packing"].round(2)
    mo["labels"]          = mo["labels"].round(2)
    print("✅ line-level inside_packing + labels computed")

    # ── 4: ORDER-LEVEL costs ─────────────────────────────────────────────
    g = mo.groupby("Name", sort=False)
    order_agg = pd.DataFrame({
        "order_weight_g": g["line_weight_g"].sum(),
        "items":          g["Lineitem quantity"].sum(),
    }).reset_index()
    boxes = np.ceil(order_agg["order_weight_g"] / MASTER_PACK_CAPACITY_G)
    order_agg["master_boxes"]   = np.where(order_agg["items"] > 0, np.maximum(boxes, 1).astype(int), 0)
    order_agg["master_packing_order"] = (order_agg["master_boxes"] * MASTER_PACK_COST).round(2)
    order_agg["bill_order"]           = np.where(order_agg["items"] > 0, BILL_COST_PER_ORDER, 0.0).round(2)
    mo = mo.merge(order_agg, on="Name", how="left")
    first_line_mask = ~mo.duplicated(subset="Name", keep="first")
    mo["master_packing"] = np.where(first_line_mask, mo["master_packing_order"], 0.0)
    mo["bill"]           = np.where(first_line_mask, mo["bill_order"], 0.0)
    mo["is_first_line"]  = first_line_mask
    print(f"✅ master_packing + bill attributed to first line ({first_line_mask.sum():,} orders)")

    # ── 5: Logistics_Detail (LINE grain) ─────────────────────────────────
    detail_cols = ["Name","Created at","order_status","channel","sub_channel","city","zip",
                   "sku_raw","Lineitem name","Lineitem quantity","weight_g","pack_band",
                   "inside_packing","labels","master_packing","bill","is_first_line",
                   "order_weight_g","master_boxes","items"]
    Logistics_Detail = mo[[c for c in detail_cols if c in mo.columns]].copy()
    Logistics_Detail = Logistics_Detail.rename(columns={
        "Name":"order_id","sku_raw":"sku","Lineitem name":"product","Lineitem quantity":"qty",
        "order_weight_g":"order_total_weight_g","master_boxes":"order_master_boxes","items":"order_total_items"})
    Logistics_Detail["line_total"] = (Logistics_Detail["inside_packing"]+Logistics_Detail["labels"]
                                      +Logistics_Detail["master_packing"]+Logistics_Detail["bill"]).round(2)
    Logistics_Detail = Logistics_Detail.sort_values(["Created at","order_id"]).reset_index(drop=True)
    print(f"✅ Logistics_Detail: {len(Logistics_Detail):,} lines, {Logistics_Detail['order_id'].nunique():,} orders")

    # ── 6: Logistics_Order (ORDER grain) ─────────────────────────────────
    gg = mo.groupby("Name", sort=False)
    Logistics_Order = pd.DataFrame({
        "order_date"     : gg[DATE_BASIS].min(),
        "order_status"   : gg["order_status"].first(),
        "channel"        : gg["channel"].first(),
        "sub_channel"    : gg["sub_channel"].first(),
        "city"           : gg["city"].first(),
        "zip"            : gg["zip"].first(),
        "items"          : gg["Lineitem quantity"].sum(),
        "order_weight_g" : gg["line_weight_g"].sum(),
        "master_boxes"   : gg["master_boxes"].first(),
        "inside_packing" : gg["inside_packing"].sum(),
        "labels"         : gg["labels"].sum(),
        "master_packing" : gg["master_packing_order"].first(),
        "bill"           : gg["bill_order"].first(),
    }).reset_index().rename(columns={"Name":"order_id"})
    Logistics_Order["driver"] = 0.0
    Logistics_Order["logistics_cost"] = (Logistics_Order["inside_packing"]+Logistics_Order["master_packing"]
        +Logistics_Order["labels"]+Logistics_Order["bill"]+Logistics_Order["driver"]).round(2)
    for c in ["inside_packing","master_packing","labels","bill","driver","logistics_cost"]:
        Logistics_Order[c] = Logistics_Order[c].round(2)
    Logistics_Order["order_date"] = pd.to_datetime(Logistics_Order["order_date"]).dt.normalize()
    print(f"✅ Logistics_Order: {len(Logistics_Order):,} orders (ALL statuses)")

    # ── 7: Logistics_DOD + driver cost from Del_Boy_Record ───────────────
    bill_mask = (Logistics_Order["channel"].isin(CHANNELS_FOR_DOD)
                 & Logistics_Order["order_status"].isin(STATUSES_FOR_DOD))
    Logistics_DOD = (Logistics_Order[bill_mask].groupby("order_date")
        .agg(orders=("order_id","nunique"), items=("items","sum"),
             inside_packing=("inside_packing","sum"), master_packing=("master_packing","sum"),
             labels=("labels","sum"), bill=("bill","sum"))
        .reset_index().rename(columns={"order_date":"Date"}).sort_values("Date"))

    driver_daily = None
    if INCLUDE_DRIVER:
        try:
            ws_d = gc_l.open_by_key(DRIVER_SHEET_ID).worksheet(DRIVER_TAB)
            db_raw = get_as_dataframe(ws_d, evaluate_formulas=True).dropna(how="all")
            db_raw.columns = [re.sub(r"[^a-z0-9_]+","_",str(c).strip().lower()).strip("_") for c in db_raw.columns]
            print(f"Del_Boy_Record raw rows: {len(db_raw):,}")
            date_c = next((c for c in ["delivery_date","date"] if c in db_raw.columns), None)
            boy_c  = next((c for c in ["delivery_boy","del_boy","rider","boy"] if c in db_raw.columns), None)
            km_c   = next((c for c in ["kms","km","kilometers","distance"] if c in db_raw.columns), None)
            cost_c = next((c for c in ["delivery_cost","driver_cost","cost","total_cost"] if c in db_raw.columns), None)
            if not (date_c and cost_c):
                raise ValueError(f"Need date + cost columns. Got date={date_c}, cost={cost_c}")
            db = db_raw[[c for c in [date_c,boy_c,km_c,cost_c] if c]].copy()
            db = db.rename(columns={date_c:"Date", boy_c:"rider", km_c:"km", cost_c:"driver"})
            db["Date"] = pd.to_datetime(db["Date"], errors="coerce", dayfirst=False)
            nat_rows = db["Date"].isna().sum()
            if nat_rows:
                print(f"   dropped {nat_rows} rows with unparseable/blank date")
            db = db.dropna(subset=["Date"]).copy()
            db["Date"] = db["Date"].dt.normalize()
            db["driver"] = (db["driver"].astype(str).str.replace(r"[₹$,\s]","",regex=True)
                            .replace({"":None,"nan":None,"None":None}))
            db["driver"] = pd.to_numeric(db["driver"], errors="coerce").fillna(0)
            print(f"   parsed driver_cost: {(db['driver']>0).sum()} rows > 0 of {len(db)} dated rows")
            if "km" in db.columns:
                db["km"] = (db["km"].astype(str).str.replace(r"[,\s]","",regex=True)
                            .replace({"":None,"nan":None}))
                db["km"] = pd.to_numeric(db["km"], errors="coerce").fillna(0)
            db = db[db["driver"] > 0]
            if len(db) == 0:
                raise ValueError("No valid driver rows remained after cleaning")
            agg = {"driver":"sum"}
            if "km" in db.columns:    agg["km"]    = "sum"
            if "rider" in db.columns: agg["rider"] = "nunique"
            driver_daily = db.groupby("Date").agg(agg).reset_index()
            driver_daily = driver_daily.rename(columns={"km":"driver_km","rider":"riders_active"})
            print(f"✅ Driver cost loaded: {len(driver_daily)} days | total ₹{driver_daily['driver'].sum():,.0f}")
        except Exception as e:
            print(f"⚠️  Could not load Del_Boy_Record: {e}")
            driver_daily = None

    if driver_daily is not None and len(driver_daily):
        Logistics_DOD["Date"] = pd.to_datetime(Logistics_DOD["Date"]).dt.normalize()
        Logistics_DOD = Logistics_DOD.merge(driver_daily, on="Date", how="left")
        Logistics_DOD["driver"] = Logistics_DOD["driver"].fillna(0).round(2)
        if "driver_km" in Logistics_DOD.columns:
            Logistics_DOD["driver_km"] = Logistics_DOD["driver_km"].fillna(0).round(1)
        if "riders_active" in Logistics_DOD.columns:
            Logistics_DOD["riders_active"] = Logistics_DOD["riders_active"].fillna(0).astype(int)
        print(f"   ✅ merged: {(Logistics_DOD['driver']>0).sum():,} of {len(Logistics_DOD):,} DOD days got driver cost")
    else:
        Logistics_DOD["driver"] = 0.0

    Logistics_DOD["logistics_cost"] = (Logistics_DOD["inside_packing"]+Logistics_DOD["master_packing"]
        +Logistics_DOD["labels"]+Logistics_DOD["bill"]+Logistics_DOD["driver"]).round(2)
    for c in ["inside_packing","master_packing","labels","bill","driver","logistics_cost"]:
        Logistics_DOD[c] = Logistics_DOD[c].round(2)
    for c in ["items","orders"]:
        Logistics_DOD[c] = Logistics_DOD[c].astype(int)
    col_order = ["Date","orders","items","riders_active","driver_km",
                 "inside_packing","master_packing","labels","bill","driver","logistics_cost"]
    Logistics_DOD = Logistics_DOD[[c for c in col_order if c in Logistics_DOD.columns]]
    print(f"✅ Logistics_DOD: {len(Logistics_DOD):,} days | total ₹{Logistics_DOD['logistics_cost'].sum():,.0f}")

    # ── 9: SAVE — parquet snapshots to Drive (update-only) + Sheets push ──
    for _df, _fn in [(Logistics_Detail, "logistics_detail.parquet"),
                     (Logistics_Order,  "logistics_order.parquet"),
                     (Logistics_DOD,    "logistics_dod.parquet")]:
        _p = os.path.join(OUTPUT_DIR, _fn)
        _df.to_parquet(_p, index=False)
        drive_upload_to_folder(_p, _fn, DRIVE_FOLDER_ID)

    gc2 = pygsheets.authorize(service_account_file=SERVICE_ACCOUNT_FILE)
    shx = gc2.open(TRACKER_NAME)

    def push_clean(df, tab):
        """Delete tab if exists, recreate, write fresh → zero stale data."""
        d = df.copy()
        for col in d.select_dtypes(include=["datetime64[ns]"]).columns:
            d[col] = pd.to_datetime(d[col]).dt.strftime("%Y-%m-%d %H:%M:%S").str.replace(" 00:00:00","",regex=False)
        d = d.astype(str).replace({"nan":"","NaT":"","None":""})
        try:
            existing = shx.worksheet_by_title(tab)
            if len(shx.worksheets()) == 1:
                shx.add_worksheet("_tmp_")
            shx.del_worksheet(existing)
            print(f"   🗑️  cleared old '{tab}'")
        except pygsheets.WorksheetNotFound:
            pass
        rows = max(len(d) + 5, 50)
        cols = max(len(d.columns) + 2, 10)
        w = shx.add_worksheet(tab, rows=rows, cols=cols)
        w.set_dataframe(d, start="A1", copy_index=False, copy_head=True)
        try: w.frozen_rows = 1
        except: pass
        print(f"   ✅ {tab}: {len(d):,} rows × {len(d.columns)} cols (fresh)")

    push_clean(Logistics_DOD,    "Logistics_DOD")
    push_clean(Logistics_Detail, "Logistics_Detail")
    push_clean(Logistics_Order,  "Logistics_Order")
    try:
        tmp = shx.worksheet_by_title("_tmp_")
        if len(shx.worksheets()) > 1: shx.del_worksheet(tmp)
    except pygsheets.WorksheetNotFound:
        pass

    # ── 10: VALIDATION ────────────────────────────────────────────────────
    print("\nCHECKS"); print("-"*60)
    print(f"1. Detail rows == master rows          : {len(Logistics_Detail)==len(mo)}")
    d_inside = round(Logistics_Detail['inside_packing'].sum(), 0)
    o_inside = round(Logistics_Order['inside_packing'].sum(), 0)
    print(f"2. Detail inside_packing == Order      : {d_inside==o_inside}  (₹{d_inside:,.0f})")
    d_master = round(Logistics_Detail['master_packing'].sum(), 0)
    o_master = round(Logistics_Order['master_packing'].sum(), 0)
    print(f"3. Detail master_packing == Order      : {d_master==o_master}  (₹{d_master:,.0f})")
    d_bill = round(Logistics_Detail['bill'].sum(), 0)
    o_bill = round(Logistics_Order['bill'].sum(), 0)
    print(f"4. Detail bill == Order                : {d_bill==o_bill}  (₹{d_bill:,.0f})")
    recomp = (Logistics_Order[['inside_packing','master_packing','labels','bill','driver']].sum(axis=1)).round(2)
    print(f"5. Components sum to logistics_cost    : {np.allclose(recomp, Logistics_Order['logistics_cost'])}")

    print("✓ STAGE 2 DONE.")
    STAGE_STATUS["logistics"] = "OK"
except Exception as e:
    STAGE_STATUS["logistics"] = f"FAILED: {e}"
    print(f"\n❌ STAGE 2 (Logistics) FAILED: {e}")
    traceback.print_exc()


# ══════════════════════════════════════════════════════════════════════════
# STAGE 3 — MARKETING COST COMBINER
# Input : Marketing_Google_Ads / Marketing_Meta_Ads / Marketing_WATI tabs
# Output: Marketing_Cost tab  (Date | Spend | Channel — long format)
# ══════════════════════════════════════════════════════════════════════════
print("\n" + "="*70)
print("📣 STAGE 3: MARKETING COST COMBINER")
print("="*70)

try:
    SHEET_NAME = "Analytics Tracker | DGF"
    GOOGLE_TAB = "Marketing_Google_Ads"
    META_TAB   = "Marketing_Meta_Ads"
    WATI_TAB   = "Marketing_WATI"
    OUTPUT_TAB = "Marketing_Cost"

    print("🔐 Authenticating with Google Sheets...")
    gc_m = pygsheets.authorize(service_account_file=SERVICE_ACCOUNT_FILE)
    sh_m = gc_m.open(SHEET_NAME)
    print(f"✅ Connected to: {SHEET_NAME}")

    def _fix_rows(data, headers):
        fixed = []
        for row in data:
            row = row[:len(headers)]
            row += [''] * (len(headers) - len(row))
            fixed.append(row)
        return fixed

    # ── GOOGLE ADS (headers on row 2, data from row 3) ────────────────────
    print(f"\n📥 Loading {GOOGLE_TAB}...")
    google_raw = sh_m.worksheet_by_title(GOOGLE_TAB).get_all_values()
    google_headers = [str(x).strip() for x in google_raw[1]]
    google_df = pd.DataFrame(_fix_rows(google_raw[2:], google_headers), columns=google_headers)
    google_df = google_df.loc[:, ~google_df.columns.duplicated()]
    google_df = google_df[['Day', 'Cost']].copy()
    google_df['Cost'] = google_df['Cost'].astype(str).str.replace(',', '', regex=False)
    google_df['Day']  = pd.to_datetime(google_df['Day'], errors='coerce')
    google_df['Cost'] = pd.to_numeric(google_df['Cost'], errors='coerce').fillna(0)
    google_df = google_df.dropna(subset=['Day'])
    google_daily = google_df.groupby('Day', as_index=False)['Cost'].sum()
    google_daily.columns = ['Date', 'Spend']
    google_daily['Channel'] = 'Google Ads'
    print(f"✅ Google total: ₹{google_daily['Spend'].sum():,.0f} over {len(google_daily)} days")

    # ── META ADS (headers row 2; use columns A and D) ─────────────────────
    print(f"\n📥 Loading {META_TAB}...")
    meta_raw = sh_m.worksheet_by_title(META_TAB).get_all_values()
    meta_headers = [str(x).strip() for x in meta_raw[1]]
    meta_df = pd.DataFrame(_fix_rows(meta_raw[2:], meta_headers), columns=meta_headers)
    meta_df = meta_df.loc[:, ~meta_df.columns.duplicated()]
    meta_df = meta_df.iloc[:, [0, 3]].copy()
    meta_df.columns = ['Day', 'Spend']
    meta_df['Spend'] = meta_df['Spend'].astype(str).str.replace(',', '', regex=False)
    meta_df['Day']   = pd.to_datetime(meta_df['Day'], errors='coerce')
    meta_df['Spend'] = pd.to_numeric(meta_df['Spend'], errors='coerce').fillna(0)
    meta_df = meta_df.dropna(subset=['Day'])
    meta_daily = meta_df.groupby('Day', as_index=False)['Spend'].sum()
    meta_daily.columns = ['Date', 'Spend']
    meta_daily['Channel'] = 'Meta Ads'
    print(f"✅ Meta total: ₹{meta_daily['Spend'].sum():,.0f} over {len(meta_daily)} days")

    # ── WATI (headers on row 1; Day + Final_Cost) ─────────────────────────
    print(f"\n📥 Loading {WATI_TAB}...")
    wati_raw = sh_m.worksheet_by_title(WATI_TAB).get_all_values()
    wati_headers = wati_raw[0]
    seen, unique_headers = {}, []
    for h in wati_headers:
        if h in seen:
            seen[h] += 1
            unique_headers.append(f"{h}_{seen[h]}")
        else:
            seen[h] = 0
            unique_headers.append(h)
    wati_df = pd.DataFrame(_fix_rows(wati_raw[1:], unique_headers), columns=unique_headers)
    wati_df = wati_df[['Day', 'Final_Cost']].copy()
    wati_df.columns = ['Day', 'Spend']
    wati_df['Spend'] = wati_df['Spend'].astype(str).str.replace(',', '', regex=False)
    wati_df['Day']   = pd.to_datetime(wati_df['Day'], errors='coerce')
    wati_df['Spend'] = pd.to_numeric(wati_df['Spend'], errors='coerce').fillna(0)
    wati_df = wati_df.dropna(subset=['Day'])
    wati_daily = wati_df.groupby('Day', as_index=False)['Spend'].sum()
    wati_daily.columns = ['Date', 'Spend']
    wati_daily['Channel'] = 'Whatsapp'
    print(f"✅ WATI total: ₹{wati_daily['Spend'].sum():,.0f} over {len(wati_daily)} days")

    # ── FINAL MERGE + PUSH ────────────────────────────────────────────────
    marketing_cost = pd.concat([google_daily, meta_daily, wati_daily], ignore_index=True)
    marketing_cost = marketing_cost.sort_values(['Date', 'Channel']).reset_index(drop=True)
    marketing_cost['Spend'] = marketing_cost['Spend'].round(2)
    print("\n💰 Channel totals:")
    print(marketing_cost.groupby('Channel')['Spend'].sum().round(0).to_string())

    print(f"\n📤 Pushing to '{OUTPUT_TAB}'...")
    try:
        wks_m = sh_m.worksheet_by_title(OUTPUT_TAB)
    except Exception:
        wks_m = sh_m.add_worksheet(OUTPUT_TAB)
        print("   Created new tab")
    df_push_m = marketing_cost.copy()
    df_push_m['Date'] = df_push_m['Date'].dt.strftime('%Y-%m-%d')
    wks_m.resize(rows=len(df_push_m) + 10, cols=10)
    wks_m.clear(start='A1')
    wks_m.set_dataframe(df_push_m, start='A1', copy_index=False, copy_head=True)
    print(f"✅ {df_push_m.shape[0]} rows pushed | range {df_push_m['Date'].iloc[0]} → {df_push_m['Date'].iloc[-1]}")

    print("✓ STAGE 3 DONE.")
    STAGE_STATUS["marketing"] = "OK"
except Exception as e:
    STAGE_STATUS["marketing"] = f"FAILED: {e}"
    print(f"\n❌ STAGE 3 (Marketing) FAILED: {e}")
    traceback.print_exc()

# ══════════════════════════════════════════════════════════════════════════
# FINAL SUMMARY — exit 1 if anything failed (GitHub run turns red)
# ══════════════════════════════════════════════════════════════════════════
print("\n" + "="*70)
print("📊 OPS PIPELINE SUMMARY")
print("="*70)
any_failed = False
for stage, status in STAGE_STATUS.items():
    icon = "✅" if status == "OK" else "❌"
    print(f"   {icon} {stage:12s}: {status}")
    if status != "OK":
        any_failed = True
print("="*70)
if any_failed:
    print("❌ One or more stages failed — see logs above")
    sys.exit(1)
print("✅✅✅ ALL STAGES COMPLETE ✅✅✅")
