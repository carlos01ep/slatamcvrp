import os
import sys
import json
import time
import asyncio
import aiohttp
import tldextract
from serpapi import GoogleSearch
import csv
import re
from datetime import datetime

# --- Directorio base del proyecto ---
BASE_DIR = os.getcwd()
sys.path.append(BASE_DIR)

# --- Archivos con rutas absolutas ---
CONFIG_PATH = os.path.join(BASE_DIR, "scrapinglatam", "crawler_config.json")
DEFAULT_CATEGORIES_PATH = os.path.join(BASE_DIR, "scrapinglatam", "default_categories.json")
OUTPUT_CSV = os.path.join(BASE_DIR, "scrapinglatam", "latam_leads.csv")
AUDIT_PATH = os.path.join(BASE_DIR, "scrapinglatam", "audits", "latam_audit.ndjson")

# --- Constantes por defecto del crawler ---
SERPAPI_KEY = os.environ.get("SERPAPI_KEY")

COUNTRIES_QUERY = [
    'site:.ar', 'site:.cl', 'site:.co', 'site:.pe', 'site:.uy',
    'site:.bo', 'site:.py', 'site:.ve', 'site:.ec'
]

def load_defaults(path, fallback):
    """Carga un JSON con lista de defaults, si no existe devuelve fallback."""
    if os.path.exists(path):
        try:
            with open(path, "r", encoding="utf-8") as fh:
                data = json.load(fh)
            if isinstance(data, list):
                return data
        except Exception as e:
            print(f"[DEFAULTS] Error leyendo {path}: {e}")
    return fallback

# --- Cargar categorías por defecto desde JSON ---
CATEGORIES = load_defaults(DEFAULT_CATEGORIES_PATH, [
    "universidad",
    "club de golf",
    "club deportivo",
    "empresa"
])

MAX_QUERIES = 36
RESULTS_PER_QUERY = 20
REQUERY_TTL_DAYS = 0  # si >0, reconsulta dominios tras X días

# --- Overrides opcionales desde JSON ---
def _override_globals(d):
    g = globals()
    if "COUNTRIES_QUERY" in d and isinstance(d["COUNTRIES_QUERY"], list):
        g["COUNTRIES_QUERY"] = d["COUNTRIES_QUERY"]
    if "CATEGORIES" in d and isinstance(d["CATEGORIES"], list):
        g["CATEGORIES"] = d["CATEGORIES"]
    for k in ["MAX_QUERIES", "RESULTS_PER_QUERY", "REQUERY_TTL_DAYS"]:
        if k in d and d[k] is not None:
            g[k] = d[k]
    if "OUTPUT_CSV" in d and isinstance(d["OUTPUT_CSV"], str) and d["OUTPUT_CSV"].strip():
        g["OUTPUT_CSV"] = os.path.join(BASE_DIR, "scrapinglatam", d["OUTPUT_CSV"].strip())

def load_config_overrides():
    if os.path.exists(CONFIG_PATH):
        try:
            with open(CONFIG_PATH, "r", encoding="utf-8") as fh:
                data = json.load(fh)
            _override_globals(data or {})
            print(f"[CFG] Overrides aplicados desde {CONFIG_PATH}")
            print(f"[CFG] COUNTRIES_QUERY = {globals().get('COUNTRIES_QUERY')}")
            print(f"[CFG] CATEGORIES = {globals().get('CATEGORIES')}")
        except Exception as e:
            print(f"[CFG] No se pudo leer {CONFIG_PATH}: {e}")
    else:
        print("[CFG] Sin overrides; usando constantes por defecto.")

# ---- Esquema CSV consistente ----
FIELDNAMES = [
    "query",
    "country",
    "category",
    "domain",
    "homepage_url",
    "http_status",
    "duration_ms",
    "emails_all",
    "email_best",
    "phones",
    "priority",
    "last_seen",
    "email_sent"
]

def ensure_dir_for(path: str):
    d = os.path.dirname(path)
    if d:
        os.makedirs(d, exist_ok=True)

def read_existing_header(path):
    if not os.path.exists(path) or os.path.getsize(path) == 0:
        return None
    try:
        with open(path, "r", encoding="utf-8-sig", newline="") as f:
            reader = csv.reader(f)
            header = next(reader, None)
            return header
    except Exception:
        return None

def open_csv_with_schema(path, fieldnames):
    ensure_dir_for(path)
    existing = read_existing_header(path)
    if existing is None:
        f = open(path, "w", encoding="utf-8-sig", newline="")
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        f.flush()
        return f, writer
    if list(existing) != list(fieldnames):
        base, ext = os.path.splitext(path)
        rotated = f"{base}_OLD_{datetime.now().strftime('%Y%m%d_%H%M%S')}{ext}"
        os.rename(path, rotated)
        print(f"[CSV] Encabezado distinto. Archivo antiguo rotado a: {rotated}")
        f = open(path, "w", encoding="utf-8-sig", newline="")
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        f.flush()
        return f, writer
    f = open(path, "a", encoding="utf-8-sig", newline="")
    writer = csv.DictWriter(f, fieldnames=fieldnames)
    return f, writer

# --- Control de dominios procesados ---
seen_domains = {}  # dominio: timestamp última consulta

def load_seen_domains():
    """Carga dominios ya procesados desde el CSV existente."""
    if os.path.exists(OUTPUT_CSV):
        try:
            with open(OUTPUT_CSV, newline="", encoding="utf-8-sig") as fh:
                reader = csv.DictReader(fh)
                for row in reader:
                    if "domain" in row and row["domain"]:
                        ts = None
                        if "last_seen" in row and row["last_seen"]:
                            try:
                                ts = datetime.fromisoformat(row["last_seen"]).timestamp()
                            except Exception:
                                ts = time.time()
                        else:
                            ts = time.time()
                        seen_domains[row["domain"].lower()] = ts
            print(f"[DOMAINS] Precargados {len(seen_domains)} dominios desde {OUTPUT_CSV}")
        except Exception as e:
            print(f"[DOMAINS] Error al precargar dominios: {e}")

def should_process(domain: str) -> bool:
    """Decide si un dominio debe procesarse según TTL."""
    now = time.time()
    if domain not in seen_domains:
        return True
    if REQUERY_TTL_DAYS <= 0:
        return False
    last_ts = seen_domains[domain]
    days = (now - last_ts) / (60*60*24)
    return days >= REQUERY_TTL_DAYS

def mark_processed(domain: str):
    seen_domains[domain] = time.time()

# --- Generación de queries ---
def get_query_permutations():
    queries = []
    for country in COUNTRIES_QUERY:
        for category in CATEGORIES:
            queries.append(f"{category} {country}")
    return queries

# --- Separar categoría y país de un query ---
def split_query(query: str):
    parts = query.split()
    category = parts[0] if parts else ""
    country_code = ""
    for p in parts:
        if p.startswith("site:."):
            country_code = p.replace("site:.", "")
            break
    return category, country_code

# Regex
EMAIL_RE = re.compile(r"[A-Za-z0-9._%+\-]+@[A-Za-z0-9.\-]+\.[A-Za-z]{2,}")
PHONE_RE = re.compile(r"(?:\+?\d{1,3}[\s\-\.]?)?(?:\(?\d{2,4}\)?[\s\-\.]?)\d{3,4}[\s\-\.]?\d{3,4}")

EMAIL_AVOID = ("noreply", "no-reply", "donotreply", "do-not-reply", "webmaster", "postmaster", "abuse")
EMAIL_PREFER = ("contacto", "contact", "info", "comercial", "ventas", "sales", "admisiones", "secretaria", "general", "prensa", "comunicacion", "informes")

def clean_emails(emails):
    out = []
    seen = set()
    for e in emails:
        el = e.strip().strip('.,;:()[]<>"\'').lower()
        if any(b in el for b in ("example.",)):
            continue
        if el not in seen:
            seen.add(el)
            out.append(el)
    return out

def pick_best_email(emails, domain):
    if not emails:
        return ""
    scored = []
    for e in emails:
        low = e.lower()
        if any(bad in low for bad in EMAIL_AVOID):
            continue
        prefer_score = -1
        for idx, kw in enumerate(EMAIL_PREFER):
            if kw in low:
                prefer_score = idx
                break
        same_domain = 0
        try:
            if domain and low.endswith("@" + domain):
                same_domain = -1
        except Exception:
            pass
        local_len = len(low.split("@")[0])
        scored.append((prefer_score, same_domain, local_len, e))
    if not scored:
        return emails[0]
    scored.sort(key=lambda t: (t[0] if t[0] >= 0 else 999,
