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
                print(f"[DEFAULTS] {len(data)} categorías cargadas desde {path}")
                return data
            else:
                print(f"[DEFAULTS] Formato inválido en {path}, usando fallback")
        except Exception as e:
            print(f"[DEFAULTS] Error leyendo {path}: {e}")
    else:
        print(f"[DEFAULTS] No se encontró {path}, usando fallback")
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
REQUERY_TTL_DAYS = 0 # si >0, reconsulta dominios tras X días

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
seen_domains = {} # dominio: timestamp última consulta

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
    """
    Separa la categoría y el código de país del query.
    IMPORTANTE: La categoría ahora es la frase completa sin el 'site:.xx'.
    """
    # Inicialmente, la categoría es la consulta completa
    category = query
    country_code = ""

    # Buscamos el código de país (site:.xx)
    for part in query.split():
        if part.startswith("site:."):
            country_code = part.replace("site:.", "")
            
            # Eliminamos el código de país de la categoría y limpiamos espacios
            category = category.replace(part, "").strip()
            # Si la categoría queda vacía (solo se buscó el país), usamos el query original
            if not category:
                category = query
            break
            
    # También limpiamos espacios extra que pudieron quedar
    category = re.sub(r'\s+', ' ', category).strip()
    
    # Manejar el caso donde el query solo contenía la categoría y no el país
    if not country_code and ' ' in category:
        # En este caso, la CATEGORY es la consulta completa, que ya está asignada
        # y no hay código de país (country_code seguirá siendo "")
        pass
        
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
    scored.sort(key=lambda t: (t[0] if t[0] >= 0 else 999, t[1], t[2]))
    return scored[0][3]

async def fetch_serpapi(query, params):
    try:
        search = GoogleSearch(params)
        results = search.get_dict()
        return results.get("organic_results", [])
    except Exception as e:
        print(f"[ERROR] SerpAPI para '{query}': {e}")
        return []

async def fetch_website_emails(session, url, priority):
    domain_info = tldextract.extract(url)
    domain = (domain_info.domain + "." + domain_info.suffix).lower()

    if not should_process(domain):
        print(f"[SKIP] Dominio ya procesado recientemente: {domain}")
        return None

    start_time = time.time()
    http_status = None
    exclusion_flag = 'N'
    emails_found = []
    phones_found = []

    try:
        # Usar un user-agent común para evitar bloqueos
        headers = {'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36'}
        async with session.get(url, ssl=False, timeout=15, headers=headers) as response:
            http_status = response.status
            content = await response.text(errors="ignore")
            emails_found = clean_emails(EMAIL_RE.findall(content))
            phones_found = list(dict.fromkeys(m.strip() for m in PHONE_RE.findall(content)))
    except asyncio.TimeoutError:
        http_status = "Timeout"
        exclusion_flag = 'Y'
        print(f"[WEB] Timeout al acceder a {url}")
    except aiohttp.ClientError as e:
        http_status = "Error"
        exclusion_flag = 'Y'
        print(f"[WEB] Error al acceder a {url}: {e}")
    except Exception as e:
        http_status = "Error"
        exclusion_flag = 'Y'
        print(f"[WEB] Error genérico en {url}: {e}")

    duration_ms = int((time.time() - start_time) * 1000)
    email_best = pick_best_email(emails_found, domain) if emails_found else ""

    ensure_dir_for(AUDIT_PATH)
    audit_event = {
        "timestamp": time.strftime("%Y-%m-%dT%H:%M:%S", time.gmtime()),
        "domain": domain,
        "url": url,
        "http_status": http_status,
        "duration_ms": duration_ms,
        "emails_found": emails_found,
        "email_best": email_best,
        "phones_found": phones_found,
        "priority": priority,
        "exclusion_flag": exclusion_flag,
        "last_seen": datetime.now().isoformat(),
    }
    with open(AUDIT_PATH, 'a', encoding='utf-8') as f:
        f.write(json.dumps(audit_event, ensure_ascii=False) + '\n')

    if not emails_found:
        return None

    mark_processed(domain)

    return {
        "query": "",
        "country": "",
        "category": "",
        "domain": domain,
        "homepage_url": url,
        "http_status": http_status,
        "duration_ms": duration_ms,
        "emails_all": ", ".join(emails_found),
        "email_best": email_best,
        "phones": ", ".join(phones_found),
        "priority": priority,
        "last_seen": datetime.now().isoformat(),
        "email_sent": "No"
    }

async def process_query(session, query, params, csv_writer):
    print(f"[QUERY] Buscando para: '{query}'")
    search_results = await fetch_serpapi(query, params)
    if not search_results:
        print(f"[QUERY] Sin resultados para: '{query}'")
        return

    tasks = []
    for result in search_results:
        url = result.get("link")
        if url:
            tasks.append(fetch_website_emails(session, url, priority=result.get("position")))

    results = await asyncio.gather(*tasks)

    for row in results:
        if not row:
            continue

        # Añadir datos de query
        row["query"] = query
        category, country = split_query(query)
        # ESTA LÍNEA AHORA GUARDA LA CATEGORÍA COMPLETA (Ej: "futbol americano")
        row["category"] = category 
        row["country"] = country
        row["email_sent"] = "No"

        # Asegurar todos los campos
        for k in FIELDNAMES:
            row.setdefault(k, "")
        csv_writer.writerow(row)

async def main():
    load_config_overrides()
    if not SERPAPI_KEY:
        print("[ERROR] Debes definir SERPAPI_KEY en el entorno.")
        return

    load_seen_domains()

    queries_to_run = get_query_permutations()
    queries_to_run = queries_to_run[:MAX_QUERIES]

    ensure_dir_for(AUDIT_PATH)
    csvfile, csv_writer = open_csv_with_schema(OUTPUT_CSV, FIELDNAMES)

    try:
        # Usamos un límite de conexiones para no saturar
        connector = aiohttp.TCPConnector(limit=20)
        async with aiohttp.ClientSession(connector=connector) as session:
            for query in queries_to_run:
                params = {
                    "engine": "google",
                    "q": query,
                    "api_key": SERPAPI_KEY,
                    "num": RESULTS_PER_QUERY,
                }
                await process_query(session, query, params, csv_writer)
                csvfile.flush()
                time.sleep(1) # Pequeña pausa entre consultas a SerpAPI
    finally:
        try:
            csvfile.flush()
            csvfile.close()
        except Exception:
            pass

    print(f"[INFO] Proceso completado. Resultados guardados en {OUTPUT_CSV}")

if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        print("\n[INFO] Proceso detenido por el usuario.")
