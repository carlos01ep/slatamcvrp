import os
import json
import time
import subprocess
import signal
import streamlit as st
import sys
import pandas as pd
# Asegúrate de que streamlit_tags esté instalado: pip install streamlit-tags
from streamlit_tags import st_tags

# 🔹 Directorio base del proyecto (raíz del repo en Streamlit Cloud)
BASE_DIR = os.getcwd()
sys.path.append(BASE_DIR)

# --- Rutas de Archivos ---
# Es crucial que estos archivos existan en la estructura esperada (scrapinglatam/)
CONFIG_PATH = os.path.join(BASE_DIR, "scrapinglatam", "crawler_config.json")
OUTPUT_CSV = os.path.join(BASE_DIR, "scrapinglatam", "latam_leads.csv")
AUDIT_PATH = os.path.join(BASE_DIR, "scrapinglatam", "audits", "latam_audit.ndjson")
SCRIPT = os.path.join(BASE_DIR, "scrapinglatam", "latam_lead_crawler_serpapi.py")
STYLES_PATH = os.path.join(BASE_DIR, "scrapinglatam", "styles.css")
DEFAULT_CATEGORIES_PATH = os.path.join(BASE_DIR, "scrapinglatam", "default_categories.json")


DEFAULT_COUNTRIES = [
    'site:.ar', 'site:.cl', 'site:.co', 'site:.pe', 'site:.uy',
    'site:.bo', 'site:.py', 'site:.ve', 'site:.ec'
]

# --- Cargar categorías por defecto desde un JSON externo ---
def load_default_categories():
    """Carga categorías desde el JSON o usa un fallback."""
    # 📌 CORRECCIÓN: Usamos la ruta constante para consistencia.
    if os.path.exists(DEFAULT_CATEGORIES_PATH):
        try:
            with open(DEFAULT_CATEGORIES_PATH, "r", encoding="utf-8") as fh:
                data = json.load(fh)
                if isinstance(data, list):
                    return data
        except Exception as e:
            st.warning(f"No se pudo leer {os.path.basename(DEFAULT_CATEGORIES_PATH)}: {e}")
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
# 📌 NOTA: Aquí se asume que styles.css está en el directorio raíz.
# Si está dentro de 'scrapinglatam', usa STYLES_PATH
if os.path.exists("styles.css"):
    with open("styles.css") as f:
        st.markdown(f"<style>{f.read()}</style>", unsafe_allow_html=True)

# --- Cargar config guardada ---
def load_config():
    """Carga la configuración del crawler."""
    if os.path.exists(CONFIG_PATH):
        try:
            with open(CONFIG_PATH, "r", encoding="utf-8") as fh:
                return json.load(fh)
        except Exception:
            # Si falla la carga, retorna un diccionario vacío
            return {}
    return {}

config = load_config()

# --- Función para guardar la configuración ---
def write_config(countries_codes, categories, max_queries, results_per_query):
    """Guarda la configuración actual en el ruta definida."""
    cfg = {
        "COUNTRIES_QUERY": countries_codes,
        "CATEGORIES": categories,
        "MAX_QUERIES": int(max_queries),
        "RESULTS_PER_QUERY": int(results_per_query),
        "OUTPUT_CSV": OUTPUT_CSV, # Usar la constante global
    }
    with open(CONFIG_PATH, "w", encoding="utf-8") as fh:
        json.dump(cfg, fh, ensure_ascii=False, indent=2)

# --- CALLBACK DEL BOTÓN (Define el valor de todos los países) ---
def select_all_countries():
    """Define el estado de sesión de países con todos los disponibles.
       Se ejecuta con on_click para evitar el error de StreamlitAPIException."""
    st.session_state["countries_ui"] = list(COUNTRY_MAP.keys())


# ------------------------------------------------------------------
# 🔹 CONFIGURACIÓN EN EL SIDEBAR
# ------------------------------------------------------------------
st.sidebar.header("Configuración")
serpapi_key = st.sidebar.text_input("SERPAPI_KEY", type="password", help="Tu API key de SerpAPI")

# --- Países ---
countries_default = config.get("COUNTRIES_QUERY", DEFAULT_COUNTRIES)

# 📌 Inicializar el estado de sesión si no existe.
if "countries_ui" not in st.session_state:
    st.session_state["countries_ui"] = [
        name for name, code in COUNTRY_MAP.items() if code in countries_default
    ]

with st.sidebar:
    st.subheader("Países activos")

    # 📌 SOLUCIÓN AL BUG DE REAPARICIÓN: Usamos la misma clave.
    # Streamlit maneja la persistencia automáticamente.
    st.multiselect(
        "Selecciona los países",
        options=list(COUNTRY_MAP.keys()),
        # Lee el estado de sesión
        default=st.session_state["countries_ui"], 
        # La clave es la misma que la variable de sesión
        key="countries_ui" 
    )

    # 🔹 Botón "Todos los países"
    # 📌 SOLUCIÓN AL ERROR DEL BOTÓN: Usamos on_click para ejecutar la modificación 
    # del estado de forma segura (callback).
    if st.button("🌍 Seleccionar Todos los Países", on_click=select_all_countries):
        # El cuerpo del if se ejecuta solo si el botón es presionado, 
        # pero la acción de modificar el estado ya se hizo en el callback.
        st.rerun() # Forzar rerun para que el multiselect se actualice con el nuevo estado

# 🔹 Convertir nombres a códigos site:.xx (usando el estado final del multiselect)
countries_codes = [COUNTRY_MAP[name] for name in st.session_state["countries_ui"]]


# --- Categorías ---
categories_default = config.get("CATEGORIES", DEFAULT_CATEGORIES)
if "categories" not in st.session_state:
    st.session_state["categories"] = categories_default.copy()

with st.sidebar:
    st.subheader("Categorías activas")
    # st_tags actualiza st.session_state["categories"]
    st.session_state["categories"] = st_tags(
        label="",
        text="Escribe y pulsa Enter para añadir",
        value=st.session_state["categories"],
        suggestions=DEFAULT_CATEGORIES,
        maxtags=50,
        key="categories_tags" # Usar una key única
    )

    with st.expander("✨ Sugerencias rápidas", expanded=False):
        suggested = [c for c in DEFAULT_CATEGORIES if c not in st.session_state["categories"]]
        if suggested:
            # Layout en columnas para los botones de sugerencia
            cols_per_row = 3
            num_rows = (len(suggested) + cols_per_row - 1) // cols_per_row
            
            for i in range(num_rows):
                cols = st.columns(cols_per_row)
                for j in range(cols_per_row):
                    idx = i * cols_per_row + j
                    if idx < len(suggested):
                        cat = suggested[idx]
                        with cols[j]:
                            if st.button(cat, key=f"suggest_cat_{cat}", use_container_width=True):
                                # Actualizar el estado de sesión y forzar rerun
                                st.session_state["categories"] = st.session_state["categories"] + [cat]
                                st.rerun()
        else:
            st.caption("✅ Todas las categorías ya están activas")

# --- Otros parámetros ---
colA, colB = st.sidebar.columns(2)

with colA:
    # 🔹 Calcular dinámicamente MAX_QUERIES (default)
    dynamic_max_queries = max(1, len(st.session_state["countries_ui"]) * len(st.session_state["categories"]))

    # 🔹 Permitir al usuario sobrescribir el valor si quiere
    max_queries_input = st.number_input(
        "NÚMERO DE CONSULTAS",
        min_value=1, max_value=500,
        value=config.get("MAX_QUERIES", dynamic_max_queries),
        step=1,
        key="max_queries_input" # Usar una key
    )
    max_queries = max_queries_input


with colB:
    results_per_query = st.number_input(
        "RESULTS_PER_QUERY", min_value=1, max_value=100,
        value=config.get("RESULTS_PER_QUERY", 20), step=1,
        key="results_per_query_input" # Usar una key
    )

# 📌 LÓGICA DE GUARDADO DE CONFIG: Guardar si detectamos un cambio en los parámetros configurables
current_config = {
    "COUNTRIES_QUERY": countries_codes,
    "CATEGORIES": st.session_state.get("categories", []),
    "MAX_QUERIES": int(max_queries),
    "RESULTS_PER_QUERY": int(results_per_query),
}

if config.get("COUNTRIES_QUERY") != current_config["COUNTRIES_QUERY"] or \
   config.get("CATEGORIES") != current_config["CATEGORIES"] or \
   config.get("MAX_QUERIES") != current_config["MAX_QUERIES"] or \
   config.get("RESULTS_PER_QUERY") != current_config["RESULTS_PER_QUERY"]:
    
    # Escribir la nueva configuración si es diferente a la guardada
    write_config(countries_codes, st.session_state["categories"], max_queries, results_per_query)


def launch_crawler():
    """Lanza el script de rastreo como un subproceso."""
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

# Inicialización de session_state para logs y proceso
if "proc" not in st.session_state:
    st.session_state["proc"] = None
if "logbuf" not in st.session_state:
    st.session_state["logbuf"] = ""
if "query_count" not in st.session_state:
    st.session_state["query_count"] = 0
if "is_running" not in st.session_state:
    st.session_state["is_running"] = False


with col1:
    st.subheader("Controles")
    if st.button("🔍 Iniciar Búsqueda", use_container_width=True):
        if not serpapi_key:
            st.warning("Define **SERPAPI_KEY** para iniciar.")
        elif st.session_state.get("proc") and st.session_state["proc"].poll() is None:
            st.info("Ya hay un proceso en ejecución.")
        else:
            # 📌 CORRECCIÓN: Asegurar que el directorio 'audits' exista
            os.makedirs(os.path.dirname(AUDIT_PATH), exist_ok=True)
            
            st.session_state["proc"] = launch_crawler()
            st.session_state["logbuf"] = ""
            st.session_state["query_count"] = 0
            st.session_state["is_running"] = True
            st.success("Crawler iniciado. Recopilando logs...")
            st.rerun() # Forzar rerun para iniciar inmediatamente el bucle de logs

    if st.button("⏹️ Detener", use_container_width=True):
        if st.session_state.get("proc") and st.session_state["proc"].poll() is None:
            try:
                # Intento de parada gradual (SIGINT)
                st.session_state["proc"].send_signal(signal.SIGINT)
                
                # Esperar 1 segundo para un cierre limpio.
                # NOTA: Este time.sleep() es aceptable *solo* porque se ejecuta al presionar un botón
                # y el usuario ya está esperando una acción de terminación.
                time.sleep(1) 
                
                if st.session_state["proc"].poll() is None:
                    # Si no se ha detenido, forzar terminación
                    st.session_state["proc"].terminate()
                
                st.session_state["is_running"] = False
                st.session_state["proc"] = None
                st.success("Proceso detenido.")
                st.rerun()
            except Exception as e:
                st.error(f"No se pudo detener: {e}")
        else:
            st.info("No hay proceso de rastreo activo para detener.")

# Placeholder para el área de logs (ayuda a mantener la posición en la UI)
log_placeholder = st.empty()

with col2:
    st.subheader("📈 Estado y Logs")

    proc = st.session_state["proc"]
    is_running = st.session_state["is_running"]

    if proc and proc.poll() is None: # Proceso en ejecución
        
        # Leemos hasta 5 líneas de forma no bloqueante (mientras el proceso escriba rápido)
        lines_read = 0
        log_chunk = ""
        try:
            while lines_read < 5:
                line = proc.stdout.readline()
                if not line: # La tubería está vacía por ahora
                    break 
                log_chunk += line
                if "[QUERY]" in line:
                    st.session_state["query_count"] += 1
                lines_read += 1
        except Exception:
            # Captura un posible error si el proceso muere durante la lectura
            pass

        st.session_state["logbuf"] += log_chunk
        
        # Forzar rerun sin bloquear la UI
        st.info(f"⚙️ **Crawler en ejecución** (Consultas: {st.session_state['query_count']}/{max_queries}). Recargando...")

        st.text_area("Logs", value=st.session_state["logbuf"], height=240, key="current_logs")
        st.rerun()

    elif proc and proc.poll() is not None: # Proceso terminó
        st.session_state["is_running"] = False
        st.success("✅ Búsqueda **finalizada**. Proceso terminado con código de salida: " + str(proc.poll()))
        
        # Mostrar logs finales
        st.text_area("Logs", value=st.session_state["logbuf"], height=240, key="final_logs")
        
        # Limpiar proc para evitar re-ejecutar este bloque
        st.session_state["proc"] = None

    else: # No hay proceso activo
        if is_running:
             st.info("⚙️ **Crawler en ejecución** (estado previo).")
        else:
            st.write("📌 **Crawler detenido / Inactivo.** Pulse 'Iniciar Búsqueda' para comenzar.")
            
        if st.session_state["logbuf"]:
            st.text_area("Logs", value=st.session_state["logbuf"], height=240, key="inactive_logs")
        else:
            st.empty().text_area("Logs", value="Logs aparecerán aquí al iniciar el rastreo...", height=240, key="empty_logs")


# ------------------------------------------------------------------
# 🔹 BOTÓN DE DESCARGA EN EL SIDEBAR (al final)
# ------------------------------------------------------------------
with st.sidebar:
    st.markdown("---")
    st.subheader("Descargar resultados")
    if os.path.exists(OUTPUT_CSV):
        with open(OUTPUT_CSV, "rb") as f:
            st.download_button(
                "⬇️ Descargar CSV",
                f,
                file_name=os.path.basename(OUTPUT_CSV),
                mime="text/csv",
                use_container_width=True
            )
    else:
        st.download_button(
            "⬇️ Descargar CSV",
            data=b"",
            file_name=os.path.basename(OUTPUT_CSV),
            disabled=True,
            use_container_width=True,
            help="El archivo CSV aún no existe o está vacío."
        )

# ------------------------------------------------------------------
# 🔹 VISTA PREVIA DEL CSV MAESTRO (pantalla completa)
# ------------------------------------------------------------------
st.subheader("📂 Vista previa de Leads (CSV maestro)")
if os.path.exists(OUTPUT_CSV):
    try:
        # Usar utf-8-sig para manejar posibles marcas de orden de bytes (BOM)
        df = pd.read_csv(OUTPUT_CSV, encoding="utf-8-sig") 

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
                # 1. Ya es un nombre completo?
                if s in COUNTRY_MAP.keys():
                    return s
                # 2. Es un código site:.xx?
                s_lower = s.lower()
                if s_lower.startswith("site:."):
                    tld = s_lower.split("site:.", 1)[1].strip().lstrip(".")
                    return code_to_name.get(tld, s) # Si no lo encuentra, devuelve el original
                # 3. Es solo el código (ar, cl, etc.)?
                if len(s) == 2:
                    return code_to_name.get(s_lower, s)
                return s

            df["country"] = df["country"].apply(to_full_country_name)
            
        # Re-ordenar columnas para la vista
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

        # --- Filtros de la Vista Previa ---
        col1_f, col2_f, col3_f = st.columns(3)
        country_options = ["Todos"] + (sorted(df["País"].dropna().unique()) if "País" in df.columns else [])
        category_options = ["Todos"] + (sorted(df["Categoría"].dropna().unique()) if "Categoría" in df.columns else [])

        with col1_f:
            country_sel = st.selectbox("Filtrar por país", country_options)
        with col2_f:
            category_sel = st.selectbox("Filtrar por categoría", category_options)
        with col3_f:
            email_status = st.selectbox("Email enviado", ["Todos", "No", "Yes"])

        # Manejo de fechas
        fecha_inicio, fecha_fin = None, None
        if "Fecha" in df.columns:
            df["Fecha"] = pd.to_datetime(df["Fecha"], errors="coerce")
            
            # Limitar a fechas válidas para evitar errores de min/max en NaT
            valid_dates = df["Fecha"].dropna()

            if not valid_dates.empty:
                min_date = valid_dates.min().date()
                max_date = valid_dates.max().date()
                
                colF1, colF2 = st.columns(2)
                with colF1:
                    fecha_inicio = st.date_input("Fecha desde", value=min_date, min_value=min_date, max_value=max_date)
                with colF2:
                    fecha_fin = st.date_input("Fecha hasta", value=max_date, min_value=min_date, max_value=max_date)

        # Aplicar filtros
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
        st.caption(f"Mostrando las últimas 100 de {len(filtered)} filas filtradas (Total de registros: {len(df)})")

    except Exception as e:
        st.error(f"Error al leer el CSV. Asegúrate de que el formato de las columnas sea correcto: {e}")
else:
    st.info("No se ha generado el CSV aún o la ruta es incorrecta. Asegúrate de que exista en: `scrapinglatam/latam_leads.csv`")

# ------------------------------------------------------------------
# 🔹 AUDITORÍA (en desplegable)
# ------------------------------------------------------------------
with st.expander("📜 Auditoría (últimos 200 eventos)", expanded=False):

    if st.button("🧹 Limpiar auditoría", key="clear_btn"):
        try:
            if os.path.exists(AUDIT_PATH):
                os.remove(AUDIT_PATH)
            st.success("Auditoría limpiada.")
            st.rerun() # Forzar rerun para actualizar la vista
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
        col1_a, col2_a, col3_a, col4_a = st.columns(4)
        excl = sum(1 for r in audit_rows if r.get("exclusion_flag") == "Y")
        
        with col1_a:
            st.metric("Eventos (últimos)", len(audit_rows))
        with col2_a:
            st.metric("Exclusiones", excl)
        with col3_a:
            st.metric("Con emails", sum(1 for r in audit_rows if r.get("emails_found")))
        with col4_a:
            avg_time = int(sum(r.get("duration_ms", 0) or 0 for r in audit_rows) / max(1, len(audit_rows)))
            st.metric("Tiempo medio (ms)", avg_time)

        st.dataframe(pd.DataFrame([
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
        ]), use_container_width=True)
    else:
        st.info("Aún no hay auditoría registrada.")
