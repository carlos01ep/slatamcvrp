import os
import json
import time
import subprocess
import signal
import streamlit as st
import sys
import pandas as pd
from streamlit_tags import st_tags

BASE_DIR = os.path.dirname(os.path.abspath(__file__))  # 🔹 Carpeta donde está app.py
sys.path.append(BASE_DIR)

CONFIG_PATH = os.path.join(BASE_DIR, "crawler_config.json")
OUTPUT_CSV = os.path.join(BASE_DIR, "latam_leads.csv")
AUDIT_PATH = os.path.join(BASE_DIR, "audits", "latam_audit.ndjson")
SCRIPT = os.path.join(BASE_DIR, "latam_lead_crawler_serpapi.py")
STYLES_PATH = os.path.join(BASE_DIR, "styles.css")
DEFAULT_CATEGORIES_PATH = os.path.join(BASE_DIR, "default_categories.json")

DEFAULT_COUNTRIES = [
    'site:.ar', 'site:.cl', 'site:.co', 'site:.pe', 'site:.uy',
    'site:.bo', 'site:.py', 'site:.ve', 'site:.ec'
]

# --- Cargar categorías por defecto desde un JSON externo ---
def load_default_categories():
    path = "default_categories.json"
    if os.path.exists(path):
        try:
            with open(path, "r", encoding="utf-8") as fh:
                data = json.load(fh)
                if isinstance(data, list):
                    return data
        except Exception as e:
            st.warning(f"No se pudo leer default_categories.json: {e}")
    # fallback si no existe el archivo o da error
    return ['universidad', 'club de golf', 'club deportivo', 'empresa']

DEFAULT_CATEGORIES = load_default_categories()

COUNTRY_MAP = {
    "Argentina": "site:.ar",
    "Chile": "site:.cl",
    "Colombia": "site:.co",
    "Perú": "site:.pe",
    "Uruguay": "site:.uy",
    "Bolivia": "site:.bo",
    "Paraguay": "site:.py",
    "Venezuela": "site:.ve",
    "Ecuador": "site:.ec"
}

st.set_page_config(page_title="LATAM Lead Crawler", layout="wide")
st.title("🕸️ LATAM Lead Crawler – SerpAPI")

# --- Cargar CSS ---
if os.path.exists("styles.css"):
    with open("styles.css") as f:
        st.markdown(f"<style>{f.read()}</style>", unsafe_allow_html=True)

# --- Cargar config guardada ---
def load_config():
    if os.path.exists(CONFIG_PATH):
        try:
            with open(CONFIG_PATH, "r", encoding="utf-8") as fh:
                return json.load(fh)
        except Exception:
            return {}
    return {}

config = load_config()

# ------------------------------------------------------------------
# 🔹 CONFIGURACIÓN EN EL SIDEBAR
# ------------------------------------------------------------------
st.sidebar.header("Configuración")
serpapi_key = st.sidebar.text_input("SERPAPI_KEY", type="password", help="Tu API key de SerpAPI")

# --- Países ---
countries_default = config.get("COUNTRIES_QUERY", DEFAULT_COUNTRIES)

if "countries_ui" not in st.session_state:
    st.session_state["countries_ui"] = [
        name for name, code in COUNTRY_MAP.items() if code in countries_default
    ]
if "pending_countries_update" not in st.session_state:
    st.session_state["pending_countries_update"] = None

if st.session_state["pending_countries_update"] is not None:
    st.session_state["countries_ui"] = st.session_state["pending_countries_update"]
    st.session_state["pending_countries_update"] = None
    st.rerun()

with st.sidebar:
    st.subheader("Países activos")

    # 🔹 multiselect en lugar de st_tags
    countries = st.multiselect(
        "Selecciona los países",
        options=list(COUNTRY_MAP.keys()),
        default=st.session_state["countries_ui"],
        key="countries_ui"
    )

    # 🔹 Botón ahora dice "Todos los países"
    if st.button("🌍 Todos los países"):
        st.session_state["pending_countries_update"] = list(COUNTRY_MAP.keys())
        st.rerun()

# 🔹 Convertir nombres a códigos site:.xx
countries_codes = [COUNTRY_MAP[name] for name in st.session_state["countries_ui"]]
if config.get("COUNTRIES_QUERY") != countries_codes:
    config["COUNTRIES_QUERY"] = countries_codes
    with open(CONFIG_PATH, "w", encoding="utf-8") as fh:
        json.dump(config, fh, ensure_ascii=False, indent=2)

# --- Categorías ---
categories_default = config.get("CATEGORIES", DEFAULT_CATEGORIES)
if "categories" not in st.session_state:
    st.session_state["categories"] = categories_default.copy()
if "pending_categories_update" not in st.session_state:
    st.session_state["pending_categories_update"] = None

if st.session_state["pending_categories_update"] is not None:
    st.session_state["categories"] = st.session_state["pending_categories_update"]
    st.session_state["pending_categories_update"] = None
    st.rerun()

with st.sidebar:
    st.subheader("Categorías activas")
    categories = st_tags(
        label="",
        text="Escribe y pulsa Enter para añadir",
        value=st.session_state["categories"],
        suggestions=DEFAULT_CATEGORIES,
        maxtags=50,
        key="categories"
    )

    with st.expander("✨ Sugerencias rápidas", expanded=False):
        suggested = [c for c in DEFAULT_CATEGORIES if c not in st.session_state["categories"]]
        if suggested:
            st.markdown('<div class="chips-scope"></div>', unsafe_allow_html=True)
            with st.container():
                for cat in suggested:
                    if st.button(cat, key=f"suggest_cat_{cat}"):
                        st.session_state["pending_categories_update"] = st.session_state["categories"] + [cat]
                        st.rerun()
        else:
            st.caption("✅ Todas las categorías ya están activas")
    # 🔹 Botón de reset categorías comentado por ahora
    #if st.button("🔄 Resetear categorías"):
    #    st.session_state["pending_categories_update"] = DEFAULT_CATEGORIES.copy()
    #    st.rerun()

if config.get("CATEGORIES") != st.session_state["categories"]:
    config["CATEGORIES"] = st.session_state["categories"]
    with open(CONFIG_PATH, "w", encoding="utf-8") as fh:
        json.dump(config, fh, ensure_ascii=False, indent=2)


# --- Otros parámetros ---
colA, colB = st.sidebar.columns(2)

with colA:
    # 🔹 Calcular dinámicamente MAX_QUERIES (default)
    dynamic_max_queries = max(1, len(st.session_state["countries_ui"]) * len(st.session_state["categories"]))

    # 🔹 Permitir al usuario sobrescribir el valor si quiere
    max_queries = st.number_input(
        "NÚMERO DE CONSULTAS",
        min_value=1, max_value=500,
        value=config.get("MAX_QUERIES", dynamic_max_queries),
        step=1
    )

    # 🔹 Si no lo cambia, usar el cálculo automático
    if max_queries == config.get("MAX_QUERIES", dynamic_max_queries):
        max_queries = dynamic_max_queries

with colB:
    results_per_query = st.number_input(
        "RESULTS_PER_QUERY", min_value=1, max_value=100,
        value=config.get("RESULTS_PER_QUERY", 20), step=1
    )

output_csv = config.get("OUTPUT_CSV", OUTPUT_CSV)


# --- Guardar config ---
def write_config():
    cfg = {
        "COUNTRIES_QUERY": countries_codes,
        "CATEGORIES": st.session_state.get("categories", []),
        "MAX_QUERIES": int(max_queries),
        "RESULTS_PER_QUERY": int(results_per_query),
        "OUTPUT_CSV": output_csv.strip(),
    }
    with open(CONFIG_PATH, "w", encoding="utf-8") as fh:
        json.dump(cfg, fh, ensure_ascii=False, indent=2)

def launch_crawler():
    env = os.environ.copy()
    if serpapi_key:
        env["SERPAPI_KEY"] = serpapi_key
    return subprocess.Popen(
        [sys.executable, "-u", SCRIPT],
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,
        text=True,
        env=env,
        bufsize=1
    )

# ------------------------------------------------------------------
# 🔹 BLOQUE SUPERIOR EN MAIN: Controles (1/3) | Estado & Logs (2/3)
# ------------------------------------------------------------------
col1, col2 = st.columns([1, 2])

with col1:
    st.subheader("Controles")
    if st.button("🔍 Iniciar Búsqueda", use_container_width=True):
        write_config()
        if not serpapi_key:
            st.warning("Define SERPAPI_KEY para iniciar.")
        else:
            if st.session_state.get("proc") and st.session_state["proc"].poll() is None:
                st.info("Ya hay un proceso en ejecución.")
            else:
                st.session_state["proc"] = launch_crawler()
                st.session_state["logbuf"] = ""
                st.session_state["query_count"] = 0
                st.session_state["is_running"] = True
                st.success("Crawler iniciado.")

    if st.button("⏹️ Detener", use_container_width=True):
        if st.session_state.get("proc"):
            try:
                st.session_state["proc"].send_signal(signal.SIGINT)
                time.sleep(1)
                if st.session_state["proc"].poll() is None:
                    st.session_state["proc"].terminate()
                st.session_state["is_running"] = False
                st.success("Proceso detenido.")
            except Exception as e:
                st.error(f"No se pudo detener: {e}")

with col2:
    st.subheader("📈 Estado y Logs")
    if "proc" not in st.session_state:
        st.session_state["proc"] = None
    if "logbuf" not in st.session_state:
        st.session_state["logbuf"] = ""
    if "query_count" not in st.session_state:
        st.session_state["query_count"] = 0
    if "is_running" not in st.session_state:
        st.session_state["is_running"] = False

    proc = st.session_state["proc"]

    if proc and proc.poll() is None:  # Proceso en ejecución
        st.session_state["is_running"] = True
        st.info("⚙️ Crawler en ejecución, por favor espere...")

        try:
            for _ in range(5):
                line = proc.stdout.readline()
                if not line:
                    break
                st.session_state["logbuf"] += line
                if "[QUERY]" in line:
                    st.session_state["query_count"] += 1
        except Exception:
            pass

        st.text_area("Logs", value=st.session_state["logbuf"], height=240)
        time.sleep(1)
        st.rerun()

    elif proc and proc.poll() is not None:  # Proceso terminó
        st.session_state["is_running"] = False
        st.success("✅ Búsqueda finalizada")
        st.text_area("Logs", value=st.session_state["logbuf"], height=240)
        st.session_state["proc"] = None
        st.session_state["query_count"] = 0

    else:  # No hay proceso activo
        if st.session_state["is_running"]:
            st.info("⚙️ Crawler en ejecución, por favor espere...")
        else:
            st.write("📌 Crawler detenido.")
        if st.session_state["logbuf"]:
            st.text_area("Logs", value=st.session_state["logbuf"], height=240)

# ------------------------------------------------------------------
# 🔹 BOTÓN DE DESCARGA EN EL SIDEBAR (al final)
# ------------------------------------------------------------------
with st.sidebar:
    st.markdown("---")
    st.subheader("Descargar resultados")
    if os.path.exists(output_csv):
        with open(output_csv, "rb") as f:
            st.download_button(
                "⬇️ Descargar CSV",
                f,
                file_name=os.path.basename(output_csv),
                mime="text/csv",
                use_container_width=True
            )
    else:
        st.download_button(
            "⬇️ Descargar CSV",
            data=b"",
            file_name=os.path.basename(output_csv),
            disabled=True,
            use_container_width=True
        )

# ------------------------------------------------------------------
# 🔹 VISTA PREVIA DEL CSV MAESTRO (pantalla completa)
# ------------------------------------------------------------------
st.subheader("📂 Vista previa de Leads (CSV maestro)")
if os.path.exists(output_csv):
    try:
        df = pd.read_csv(output_csv, encoding="utf-8-sig")

        # 🔹 Quitar columnas índice sin nombre (p. ej., 'Unnamed: 0')
        df = df.loc[:, ~df.columns.str.startswith("Unnamed")]

        # 🔹 Normalizar país a nombre completo ANTES de renombrar columnas
        if "country" in df.columns:
            reverse_country_map = {v: k for k, v in COUNTRY_MAP.items()}
            code_to_name = {
                "ar": "Argentina", "cl": "Chile", "co": "Colombia",
                "pe": "Perú", "uy": "Uruguay", "bo": "Bolivia",
                "py": "Paraguay", "ve": "Venezuela", "ec": "Ecuador"
            }

            def to_full_country_name(val):
                if pd.isna(val):
                    return val
                s = str(val).strip()
                if s in COUNTRY_MAP.keys():
                    return s
                if s in reverse_country_map:
                    return reverse_country_map[s]
                s_lower = s.lower()
                if s_lower.startswith("site:."):
                    tld = s_lower.split("site:.", 1)[1].strip().lstrip(".")
                    return code_to_name.get(tld, reverse_country_map.get(f"site:.{tld}", s))
                if s_lower.startswith("."):
                    tld = s_lower.lstrip(".")
                    return code_to_name.get(tld, reverse_country_map.get(f"site:.{tld}", s))
                if len(s) == 2:
                    return code_to_name.get(s_lower, reverse_country_map.get(f"site:.{s_lower}", s))
                return s

            df["country"] = df["country"].apply(to_full_country_name)

        required_cols = ["country", "category", "domain", "email_best", "email_sent", "last_seen"]
        df = df[[c for c in required_cols if c in df.columns]]

        rename_map = {
            "country": "País",
            "category": "Categoría",
            "domain": "Dominio",
            "email_best": "Email",
            "email_sent": "Email enviado",
            "last_seen": "Fecha"
        }
        df = df.rename(columns={k: v for k, v in rename_map.items() if k in df.columns})

        col1, col2, col3 = st.columns(3)
        with col1:
            country_sel = st.selectbox(
                "Filtrar por país",
                ["Todos"] + (sorted(df["País"].dropna().unique()) if "País" in df.columns else [])
            )
        with col2:
            category_sel = st.selectbox(
                "Filtrar por categoría",
                ["Todos"] + (sorted(df["Categoría"].dropna().unique()) if "Categoría" in df.columns else [])
            )
        with col3:
            email_status = st.selectbox("Email enviado", ["Todos", "No", "Yes"])

        fecha_inicio, fecha_fin = None, None
        if "Fecha" in df.columns:
            df["Fecha"] = pd.to_datetime(df["Fecha"], errors="coerce")
            colF1, colF2 = st.columns(2)
            with colF1:
                fecha_inicio = st.date_input(
                    "Fecha desde",
                    value=(df["Fecha"].min().date() if not df["Fecha"].isna().all() else None)
                )
            with colF2:
                fecha_fin = st.date_input(
                    "Fecha hasta",
                    value=(df["Fecha"].max().date() if not df["Fecha"].isna().all() else None)
                )

        filtered = df.copy()
        if "País" in filtered.columns and country_sel != "Todos":
            filtered = filtered[filtered["País"] == country_sel]
        if "Categoría" in filtered.columns and category_sel != "Todos":
            filtered = filtered[filtered["Categoría"] == category_sel]
        if "Email enviado" in filtered.columns and email_status != "Todos":
            filtered = filtered[filtered["Email enviado"] == email_status]
        if "Fecha" in filtered.columns and fecha_inicio and fecha_fin:
            filtered = filtered[
                (filtered["Fecha"].dt.date >= fecha_inicio) &
                (filtered["Fecha"].dt.date <= fecha_fin)
            ]

        st.dataframe(filtered.tail(100), use_container_width=True)

    except Exception as e:
        st.error(f"Error al leer el CSV: {e}")
else:
    st.info("No se ha generado el CSV aún.")

# ------------------------------------------------------------------
# 🔹 AUDITORÍA (en desplegable)
# ------------------------------------------------------------------
with st.expander("📜 Auditoría (últimos 200 eventos)", expanded=False):

    if st.button("🧹 Limpiar auditoría", key="clear_btn"):
        try:
            if os.path.exists(AUDIT_PATH):
                os.remove(AUDIT_PATH)
            st.success("Auditoría limpiada.")
        except Exception as e:
            st.error(f"No se pudo limpiar: {e}")

    audit_rows = []
    if os.path.exists(AUDIT_PATH):
        try:
            with open(AUDIT_PATH, "r", encoding="utf-8") as fh:
                lines = fh.readlines()[-200:]
            for ln in lines:
                try:
                    audit_rows.append(json.loads(ln))
                except Exception:
                    continue
        except Exception:
            pass

    if audit_rows:
        col1, col2, col3, col4 = st.columns(4)
        excl = sum(1 for r in audit_rows if r.get("exclusion_flag") == "Y")
        with col1:
            st.metric("Eventos (últimos)", len(audit_rows))
        with col2:
            st.metric("Exclusiones", excl)
        with col3:
            st.metric("Con emails", sum(1 for r in audit_rows if r.get("emails_found")))
        with col4:
            st.metric(
                "Tiempo medio (ms)",
                int(sum(r.get("duration_ms", 0) or 0 for r in audit_rows) / max(1, len(audit_rows)))
            )

        st.dataframe([
            {
                "ts": r.get("timestamp"),
                "dominio": r.get("domain"),
                "http": r.get("http_status"),
                "ms": r.get("duration_ms"),
                "emails": ", ".join(r.get("emails_found", [])[:3]),
                "email_best": r.get("email_best"),
                "prio": r.get("priority"),
                "excl": r.get("exclusion_flag"),
            } for r in reversed(audit_rows)
        ], use_container_width=True)
    else:
        st.info("Aún no hay auditoría registrada.")


