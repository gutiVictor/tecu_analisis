"""
Dashboard de Análisis de Despachos TECU Aura
Versión mejorada con filtros globales y análisis completo
"""

# ──────────────────────────────────────────────────────────────────────────
# 📦 IMPORTACIONES DE LIBRERÍAS
# ──────────────────────────────────────────────────────────────────────────
import streamlit as st
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
from data_processor import DataProcessor
import io
import logging
from datetime import datetime
from typing import List, Dict, Optional
import os  # ← IMPORTANTE: Para crear carpetas

# ──────────────────────────────────────────────────────────────────────────
# ⚙️ CONFIGURACIÓN DE LOGGING (Para monitoreo y debugging)
# ──────────────────────────────────────────────────────────────────────────

# Crear carpeta de logs ANTES de configurar logging
try:
    os.makedirs("logs", exist_ok=True)  # ← Crear carpeta primero
except Exception as e:
    print(f"⚠️ No se pudo crear carpeta logs: {e}")

# Configurar logging con fallback a solo consola si falla el archivo
try:
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(levelname)s - %(message)s',
        handlers=[
            logging.FileHandler(
                f"logs/tecu_dashboard_{datetime.now().strftime('%Y%m%d')}.log",
                encoding='utf-8'
            ),
            logging.StreamHandler()
        ]
    )
except Exception as e:
    # Fallback: solo consola si falla el archivo
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(levelname)s - %(message)s',
        handlers=[logging.StreamHandler()]
    )
    print(f"⚠️ Logging en archivo falló, usando solo consola: {e}")

logger = logging.getLogger(__name__)


# ──────────────────────────────────────────────────────────────────────────
# 🎨 CONSTANTES DE CONFIGURACIÓN VISUAL Y DE NEGOCIO
# ──────────────────────────────────────────────────────────────────────────
# Colores semánticos para estados de cumplimiento (accesibles y consistentes)
COLOR_CUMPLE = '#22c55e'      # Verde: pedido cumplido dentro de SLA
COLOR_NO_CUMPLE = '#ef4444'   # Rojo: pedido fuera de SLA
COLOR_PTE = '#f59e0b'         # Ámbar: pedido pendiente (PTE)
COLOR_PRIMARY = '#6366f1'     # Índigo: color principal de la marca

# Plantilla de Plotly para modo oscuro (coherente con el diseño)
PLOTLY_TEMPLATE = 'plotly_dark'

# Umbrales de negocio para alertas automáticas (configurables)
UMBRALES_ALERTAS = {
    'cumplimiento_minimo': 80.0,      # % mínimo de cumplimiento para alerta
    'desvio_entrega_max': 5.0,        # Días máximos de desvío promedio
    'transportadora_min_perf': 60.0,  # % mínimo de desempeño por transportadora
    'pendientes_max': 10              # Máximo de pedidos PTE antes de alertar
}


# ──────────────────────────────────────────────────────────────────────────
# 🔧 FUNCIÓN AUXILIAR: Preparar datos para interactividad de clicks
# ──────────────────────────────────────────────────────────────────────────
def _preparar_datos_para_click(df_filtrado: pd.DataFrame, columnas_clave: List[str]) -> pd.DataFrame:
    """
    Crea un identificador único por fila para rastrear clicks en gráficos.
    
    Args:
        df_filtrado: DataFrame con los datos ya filtrados por el usuario
        columnas_clave: Lista de columnas que forman la clave única
        
    Returns:
        DataFrame con columna adicional '_click_id' para tracking
    """
    df_click = df_filtrado.copy()  # Evitar modificar el original
    # Concatenar valores de columnas clave como string único por fila
    df_click['_click_id'] = df_click[columnas_clave].astype(str).agg('_'.join, axis=1)
    return df_click


# ──────────────────────────────────────────────────────────────────────────
# 🔍 FUNCIÓN: Mostrar datos fuente al hacer click en gráfico (Drill-Down)
# ──────────────────────────────────────────────────────────────────────────
def mostrar_datos_fuente(
    df_filtrado: pd.DataFrame, 
    seleccion: Dict, 
    columnas_filtro: List[tuple], 
    titulo_seccion: str = "🔍 Datos Fuente"
) -> None:
    """
    Muestra en un expandable los registros que generaron el elemento clickeado.
    
    Args:
        df_filtrado: DataFrame completo con los datos filtrados
        seleccion: Dict con información del punto seleccionado (de on_select)
        columnas_filtro: Lista de tuplas [(col_df, valor_seleccion)] para filtrar
        titulo_seccion: Título personalizado para la sección de datos
    """
    # Validar que haya una selección válida con puntos
    if not seleccion or 'points' not in seleccion or not seleccion['points']:
        return
    
    punto = seleccion['points'][0]  # Tomar el primer punto seleccionado
    
    # Solo procesar si el punto tiene customdata (metadatos del gráfico)
    if 'customdata' in punto:
        with st.expander(titulo_seccion, expanded=True):
            df_resultado = df_filtrado.copy()  # Copia para no alterar original
            
            # Aplicar filtros dinámicos según los valores del punto clickeado
            for i, (col, _) in enumerate(columnas_filtro):
                if i < len(punto.get('customdata', [])):
                    valor = punto['customdata'][i]
                    if pd.notna(valor):  # Ignorar valores nulos
                        # Filtrar por coincidencia exacta de string (robusto a tipos mixtos)
                        df_resultado = df_resultado[
                            df_resultado[col].astype(str) == str(valor)
                        ]
            
            # Mostrar contador de registros encontrados
            st.caption(f"Registros que generan este punto: {len(df_resultado)}")
            
            # Seleccionar columnas relevantes para mostrar al usuario
            cols_display = [
                c for c in ['No_Orden', 'Cliente', 'Ciudad', 'Transportadora', 
                           'Fecha_Entrega', 'Cumple_NNS', 'Desvio_Entrega', 'Area_Incumple'] 
                if c in df_resultado.columns
            ]
            
            # Tabla interactiva con los 50 primeros registros
            st.dataframe(
                df_resultado[cols_display].head(50), 
                use_container_width=True, 
                hide_index=True
            )
            
            # Botón de exportación a Excel si hay datos
            if len(df_resultado) > 0:
                buf = io.BytesIO()  # Buffer en memoria para el archivo
                df_resultado.to_excel(buf, index=False, sheet_name='Datos_Fuente')
                buf.seek(0)  # Reiniciar posición del buffer para lectura
                
                st.download_button(
                    "📥 Exportar estos datos",
                    data=buf,
                    file_name="datos_fuente_seleccion.xlsx",
                    mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
                )


# ──────────────────────────────────────────────────────────────────────────
# 🎨 CONFIGURACIÓN DE PÁGINA Y ESTILOS CSS PERSONALIZADOS
# ──────────────────────────────────────────────────────────────────────────
st.set_page_config(
    page_title="TECU – Análisis de Despachos",  # Título en pestaña del navegador
    page_icon="📦",                              # Ícono de la pestaña
    layout="wide",                               # Usar ancho completo de pantalla
    initial_sidebar_state="expanded",            # Sidebar visible por defecto
)

# CSS personalizado para tema oscuro profesional y branding consistente
st.markdown("""
<style>
    /* Fondo general de la aplicación */
    .stApp { background-color: #0f1117; }

    /* Tarjetas de KPIs: gradiente, bordes y tipografía */
    [data-testid="stMetric"] {
        background: linear-gradient(135deg, #1e2130, #252840);
        border: 1px solid #2e3250;
        border-radius: 12px;
        padding: 16px 20px;
    }
    [data-testid="stMetric"] > div:first-child {
        font-size: 0.78rem;
        color: #8b9dc3;
        text-transform: uppercase;
        letter-spacing: 0.05em;
    }
    [data-testid="stMetricValue"] {
        font-size: 2rem !important;
        font-weight: 700;
        color: #e8ecf4;
    }
    
    /* Separador visual entre bloques de KPIs */
    .kpi-separator {
        border: none;
        border-top: 1px dashed #2e3250;
        margin: 10px 0 14px 0;
    }

    /* Sidebar: gradiente vertical y colores de texto */
    [data-testid="stSidebar"] {
        background: linear-gradient(180deg, #111827, #1a2035);
        border-right: 1px solid #2e3250;
    }
    [data-testid="stSidebar"] h1, 
    [data-testid="stSidebar"] h2,
    [data-testid="stSidebar"] h3 { color: #a5b4fc; }

    /* Títulos principales de la app */
    h1 { color: #e8ecf4 !important; }
    h2, h3 { color: #a5b4fc !important; }

    /* Líneas divisorias horizontales */
    hr { border-color: #2e3250; }

    /* Tarjetas de recomendaciones con borde izquierdo de color */
    .rec-card {
        background: #1a2035;
        border-left: 4px solid #6366f1;
        border-radius: 8px;
        padding: 12px 16px;
        margin-bottom: 12px;
    }
    .rec-success { border-left-color: #22c55e; }  /* Verde: éxito */
    .rec-warning { border-left-color: #f59e0b; }  /* Ámbar: advertencia */
    .rec-error   { border-left-color: #ef4444; }  /* Rojo: error */
    .rec-info    { border-left-color: #38bdf8; }  /* Azul: información */

    /* Encabezados de tablas de datos */
    .dataframe thead th { background: #252840 !important; }
</style>
""", unsafe_allow_html=True)


# ──────────────────────────────────────────────────────────────────────────
# 🛠️ FUNCIONES AUXILIARES DE UTILIDAD
# ──────────────────────────────────────────────────────────────────────────
def color_tipo(tipo: str) -> str:
    """
    Mapea tipo de mensaje a color hexadecimal para UI consistente.
    
    Args:
        tipo: String con categoría ('success', 'warning', 'error', 'info')
        
    Returns:
        String con código de color hexadecimal
    """
    return {
        'success': COLOR_CUMPLE,
        'warning': COLOR_PTE,
        'error': COLOR_NO_CUMPLE,
        'info': '#38bdf8',
    }.get(tipo, COLOR_PRIMARY)  # Fallback al color primario si no coincide


def fig_base() -> Dict:
    """
    Retorna configuración base para gráficos Plotly en modo oscuro.
    
    Returns:
        Dict con layout base reutilizable en todos los gráficos
    """
    return {
        'template': PLOTLY_TEMPLATE,           # Tema oscuro de Plotly
        'paper_bgcolor': 'rgba(0,0,0,0)',      # Fondo transparente
        'plot_bgcolor': 'rgba(0,0,0,0)',       # Área de gráfico transparente
        'font': {'color': '#e8ecf4', 'family': 'Inter, sans-serif'},  # Tipografía
        'margin': {'l': 20, 'r': 20, 't': 40, 'b': 40},  # Márgenes compactos
    }


# ──────────────────────────────────────────────────────────────────────────
# 📥 CARGA Y PROCESAMIENTO DE DATOS (CON CACHE PARA RENDIMIENTO)
# ──────────────────────────────────────────────────────────────────────────
@st.cache_data(show_spinner=False, ttl=3600)  # Cache por 1 hora para evitar reprocesar
def _cargar_df_nuclear_v7(
    archivo_bytes: bytes, 
    nombre_archivo: str, 
    sla_almacen: int = 1, 
    sla_principal: int = 3, 
    sla_otras: int = 5
) -> tuple:
    """
    Función interna con cache para cargar y procesar archivo Excel.
    
    Args:
        archivo_bytes: Contenido binario del archivo subido
        nombre_archivo: Nombre original del archivo (para logs)
        sla_almacen: Días máximos para despacho desde almacén
        sla_principal: SLA para ciudades principales (Bogotá, Medellín, Cali)
        sla_otras: SLA para otras ciudades
        
    Returns:
        Tupla (DataFrame procesado, nombre de hoja usada) o (None, None) si error
    """
    logger.info(f"Iniciando carga de archivo: {nombre_archivo}")
    
    try:
        # Leer archivo Excel desde bytes en memoria
        xl = pd.ExcelFile(io.BytesIO(archivo_bytes))

        # 🔍 Detectar automáticamente la hoja con datos (flexible a nombres variados)
        hoja = None
        for h in xl.sheet_names:
            if any(kw in h.lower() for kw in ['venta', 'base', 'despacho']):
                hoja = h
                logger.info(f"Hoja detectada: {hoja}")
                break
        if hoja is None:
            hoja = xl.sheet_names[0]  # Fallback: usar primera hoja
            logger.warning(f"Usando hoja por defecto: {hoja}")

        # 🔍 Detectar fila de encabezado dinámicamente (robusto a formatos)
        df_raw = pd.read_excel(
            io.BytesIO(archivo_bytes), 
            sheet_name=hoja, 
            header=None, 
            nrows=10  # Leer solo primeras 10 filas para detectar header
        )
        header_row = 0
        for i in range(len(df_raw)):
            # Buscar palabras clave que indiquen fila de encabezado
            row_vals = ' '.join([str(v).lower() for v in df_raw.iloc[i].values])
            if any(kw in row_vals for kw in ['fecha', 'cliente', 'ciudad', 'no orden']):
                header_row = i
                logger.info(f"Fila de encabezado detectada: {header_row}")
                break

        # Leer DataFrame completo con encabezado detectado
        df = pd.read_excel(
            io.BytesIO(archivo_bytes), 
            sheet_name=hoja, 
            header=header_row
        )
        logger.info(f"DataFrame cargado: {len(df)} filas, {len(df.columns)} columnas")

        # 🔄 Procesar datos con parámetros de SLA configurados
        from data_processor import DataProcessor as _DP
        p = _DP(df)
        df_procesado = p.procesar(sla_almacen, sla_principal, sla_otras)
        logger.info(f"Procesamiento completado: {len(df_procesado)} registros válidos")
        
        return df_procesado, hoja

    except Exception as e:
        logger.error(f"Error crítico al cargar archivo: {str(e)}", exc_info=True)
        st.error(f"❌ Error al procesar el archivo: {e}")
        return None, None


def cargar_y_procesar(
    uploaded_file, 
    sla_almacen: int = 1, 
    sla_principal: int = 3, 
    sla_otras: int = 5
) -> tuple:
    """
    Wrapper público para cargar y procesar archivo con parámetros SLA.
    
    Args:
        uploaded_file: Objeto file_uploader de Streamlit
        sla_almacen, sla_principal, sla_otras: Parámetros de configuración SLA
        
    Returns:
        Tupla (processor, df_procesado, hoja) o (None, None, None) si error
    """
    archivo_bytes = uploaded_file.getvalue()  # Convertir a bytes para cache
    df_procesado, hoja = _cargar_df_nuclear_v7(
        archivo_bytes, uploaded_file.name, sla_almacen, sla_principal, sla_otras
    )
    
    if df_procesado is None:
        return None, None, None
    
    # Crear instancia fresca de DataProcessor con datos ya procesados
    processor = DataProcessor(df_procesado)
    processor.df_procesado = df_procesado  # Asignar para acceso directo
    
    return processor, df_procesado, hoja


# ──────────────────────────────────────────────────────────────────────────
# 🎛️ SIDEBAR: FILTROS GLOBALES Y CONFIGURACIÓN
# ──────────────────────────────────────────────────────────────────────────
def sidebar_filtros(df_procesado: pd.DataFrame) -> tuple:
    """
    Renderiza sidebar con filtros globales y retorna DataFrame filtrado.
    
    Args:
        df_procesado: DataFrame original procesado
        
    Returns:
        Tupla (df_filtrado, debug_mode) con datos aplicando filtros y flag de debug
    """
    st.sidebar.markdown("## 📦 TECU Despachos")
    st.sidebar.markdown("---")

    df_f = df_procesado.copy()  # Trabajar sobre copia para no alterar original
    total_rows = len(df_procesado)

    if df_procesado is None or total_rows == 0:
        return df_f, False

    st.sidebar.markdown("### 🔍 Filtros Globales")

    # ── 📅 FILTRO POR MES (con mapeo seguro para orden cronológico) ──
    # Crear diccionario de mapeo Mes_Sort (numérico) → Mes_Label (legible)
    df_meses = df_f[['Mes_Sort', 'Mes_Label']].dropna().drop_duplicates().sort_values('Mes_Sort')
    mapa_mes = dict(zip(df_meses['Mes_Sort'].astype(str), df_meses['Mes_Label'].astype(str)))
    
    opciones_mes = list(mapa_mes.values())  # Opciones legibles para el usuario
    sel_mes = st.sidebar.multiselect(
        "📅 Mes",
        options=['Todos'] + opciones_mes,
        default=['Todos'],
        key='ms_filtro_mes',
        help="Selecciona uno o varios meses para filtrar los datos"
    )

    # ── 🚚 FILTRO POR TRANSPORTADORA ──
    opciones_transp = sorted(df_f['Transportadora'].dropna().unique().astype(str).tolist())
    sel_transp = st.sidebar.multiselect(
        "🚚 Transportadora",
        options=['Todas'] + opciones_transp,
        default=['Todas'],
        key='ms_filtro_transp',
        help="Filtra por empresa de transporte"
    )

    # ── 📍 FILTRO POR CIUDAD ──
    opciones_ciudad = sorted(df_f['Ciudad'].dropna().unique().astype(str).tolist())
    sel_ciudad = st.sidebar.multiselect(
        "📍 Ciudad",
        options=['Todas'] + opciones_ciudad,
        default=['Todas'],
        key='ms_filtro_ciudad',
        help="Filtra por ciudad de destino"
    )

    # ── 📦 NUEVO: FILTRO POR CATEGORÍA DE PRODUCTO ──
    if 'Categoria' in df_f.columns:
        opciones_cat = sorted(df_f['Categoria'].dropna().unique().astype(str).tolist())
        sel_cat = st.sidebar.multiselect(
            "📦 Categoría",
            options=['Todas'] + opciones_cat,
            default=['Todas'],
            key='ms_filtro_categoria',
            help="Filtra por tipo de producto (Superficie, Standing Desk, etc.)"
        )
    else:
        sel_cat = ['Todas']  # Fallback si columna no existe

    # ── 🏷️ NUEVO: FILTRO POR CONCEPTO (Venta vs Novedad) ──
    if 'Concepto' in df_f.columns:
        opciones_concepto = sorted(df_f['Concepto'].dropna().unique().astype(str).tolist())
        sel_concepto = st.sidebar.multiselect(
            "🏷️ Concepto",
            options=['Todos'] + opciones_concepto,
            default=['Todos'],
            key='ms_filtro_concepto',
            help="Filtra por tipo de transacción: Venta normal o Novedad"
        )
    else:
        sel_concepto = ['Todos']

    # ── 💰 NUEVO: FILTRO POR RANGO DE VALOR DESPACHO ──
    if 'Valor despacho' in df_f.columns:
        # Limpiar y convertir valores monetarios a numéricos
        df_f['Valor_num'] = pd.to_numeric(
            df_f['Valor despacho'].astype(str).str.replace(r'[^\d.]', '', regex=True), 
            errors='coerce'
        ).fillna(0)
        
        min_val, max_val = df_f['Valor_num'].min(), df_f['Valor_num'].max()
        rango_valor = st.sidebar.slider(
            "💰 Rango Valor Despacho (COP)",
            min_value=float(min_val), 
            max_value=float(max_val),
            value=(float(min_val), float(max_val)),
            key='slider_valor',
            help="Filtra por monto del despacho en pesos colombianos"
        )
    else:
        rango_valor = (0, float('inf'))  # Sin filtro si columna no existe

    # ── 🔄 APLICAR TODOS LOS FILTROS AL DATAFRAME ──
    # Filtro por Mes (comparación directa de labels para evitar errores de mapeo)
    if 'Todos' not in sel_mes and len(sel_mes) > 0:
        df_f = df_f[df_f['Mes_Label'].astype(str).isin(sel_mes)]

    # Filtro por Transportadora
    if 'Todas' not in sel_transp and len(sel_transp) > 0:
        df_f = df_f[df_f['Transportadora'].astype(str).isin(sel_transp)]

    # Filtro por Ciudad
    if 'Todas' not in sel_ciudad and len(sel_ciudad) > 0:
        df_f = df_f[df_f['Ciudad'].astype(str).isin(sel_ciudad)]

    # Filtro por Categoría (NUEVO)
    if 'Categoria' in df_f.columns and 'Todas' not in sel_cat and len(sel_cat) > 0:
        df_f = df_f[df_f['Categoria'].astype(str).isin(sel_cat)]

    # Filtro por Concepto (NUEVO)
    if 'Concepto' in df_f.columns and 'Todos' not in sel_concepto and len(sel_concepto) > 0:
        df_f = df_f[df_f['Concepto'].astype(str).isin(sel_concepto)]

    # Filtro por Rango de Valor (NUEVO)
    if 'Valor_num' in df_f.columns:
        df_f = df_f[
            (df_f['Valor_num'] >= rango_valor[0]) & 
            (df_f['Valor_num'] <= rango_valor[1])
        ]

    # ── 🛠️ HERRAMIENTAS DE DESARROLLO Y UTILIDAD ──
    st.sidebar.markdown("---")
    st.sidebar.markdown("### 🛠️ Herramientas")
    
    # Checkbox para modo debug: muestra eventos de selección en consola
    debug_mode = st.sidebar.checkbox(
        "Modo Debug (Ver Eventos)", 
        value=False,
        help="Activa logs detallados de interacciones con gráficos"
    )
    
    # Contador de registros filtrados vs totales
    curr_rows = len(df_f)
    st.sidebar.caption(f"📊 Registros: {curr_rows:,} / {total_rows:,}")
    
    # Botón para limpiar cache y recargar app (útil en desarrollo)
    if st.sidebar.button("🔄 Reiniciar App (Borrar Caché)"):
        st.cache_data.clear()
        logger.info("Cache limpiado por usuario - App reiniciada")
        st.rerun()

    return df_f, debug_mode


# ──────────────────────────────────────────────────────────────────────────
# 📊 KPIs: INDICADORES CLAVE DE DESEMPEÑO
# ──────────────────────────────────────────────────────────────────────────
def _fila_kpis(indicadores: Dict, label_prefix: str = "") -> None:
    """
    Renderiza una fila horizontal con 5 métricas KPI usando st.columns.
    
    Args:
        indicadores: Dict con métricas calculadas por DataProcessor
        label_prefix: Texto opcional para prefijar etiquetas (ej. para diferenciar global/filtrado)
    """
    cols = st.columns(5)  # Crear 5 columnas de igual ancho
    
    # Definir datos de cada KPI: (etiqueta, valor, delta, help_text)
    datos = [
        ("📦 Total Pedidos", indicadores['total_pedidos'],
         None, "Pedidos con status Entregado"),
        ("✅ % Cumplimiento NNS", f"{indicadores['pct_cumplimiento']}%",
         f"{indicadores['cumplen_nns']} cumplen", "Porcentaje dentro del SLA"),
        ("⚠️ Desvío Despacho", indicadores['con_desvio_despacho'],
         f"Prom: {indicadores['promedio_desvio_despacho']}d", "Pedidos con retraso en despacho"),
        ("🔴 Desvío Entrega", indicadores['con_desvio_entrega'],
         f"Prom: {indicadores['promedio_desvio_entrega']}d", "Pedidos fuera de SLA"),
        ("⏳ Pendientes (PTE)", indicadores['pendientes'],
         None, "Sin fecha de entrega registrada"),
    ]
    
    # Renderizar cada KPI en su columna correspondiente
    for col, (label, val, delta, help_txt) in zip(cols, datos):
        with col:
            st.metric(f"{label_prefix}{label}", val, delta, help=help_txt)


def _fila_kpis_financieros(df_filtrado: pd.DataFrame) -> None:
    """
    Renderiza fila adicional con KPIs financieros (NUEVA FUNCIÓN).
    
    Args:
        df_filtrado: DataFrame con datos filtrados para cálculos
    """
    if 'Valor despacho' not in df_filtrado.columns:
        return  # Saltar si columna financiera no existe
    
    # Limpiar y convertir valores monetarios a numéricos para cálculos
    valores = pd.to_numeric(
        df_filtrado['Valor despacho'].astype(str).str.replace(r'[^\d.]', '', regex=True), 
        errors='coerce'
    ).fillna(0)
    
    total_valor = valores.sum()
    ticket_promedio = valores.mean() if len(valores) > 0 else 0
    
    # Calcular desvío de costos si columna existe
    desvio_costo = 0
    if 'Diferencia valor real vs Estimado' in df_filtrado.columns:
        desvios = pd.to_numeric(
            df_filtrado['Diferencia valor real vs Estimado'].astype(str).str.replace(r'[^\d.-]', '', regex=True), 
            errors='coerce'
        ).fillna(0)
        desvio_costo = desvios.abs().sum()
    
    cols = st.columns(4)  # 4 columnas para KPIs financieros
    
    datos = [
        ("💰 Total Despachos", f"${total_valor:,.0f}", None, "Valor total de despachos filtrados"),
        ("📈 Ticket Promedio", f"${ticket_promedio:,.0f}", None, "Valor promedio por pedido"),
        ("⚠️ Desvío Costo", f"${desvio_costo:,.0f}", None, "Diferencia acumulada real vs estimado"),
        ("🎯 Pedidos Altos", f"{len(valores[valores > ticket_promedio*1.5]):,}", 
         None, f"Pedidos > 150% del promedio (${ticket_promedio*1.5:,.0f})"),
    ]
    
    for col, (label, val, delta, help_txt) in zip(cols, datos):
        with col:
            st.metric(label, val, delta, help=help_txt)


def mostrar_kpis(ind_global: Dict, ind_filtrado: Dict, etiqueta_filtro: str = "Selección") -> None:
    """
    Muestra KPIs en dos bloques comparativos: Global (sin filtros) vs Filtrado.
    
    Args:
        ind_global: Indicadores calculados sobre dataset completo
        ind_filtrado: Indicadores calculados sobre datos filtrados
        etiqueta_filtro: Texto descriptivo para el bloque filtrado
    """
    # ── BLOQUE GLOBAL: Métricas del dataset completo (referencia base) ──
    st.markdown(
        "<p style='margin:0 0 6px 0; color:#8b9dc3; font-size:0.78rem; "
        "text-transform:uppercase; letter-spacing:0.06em;'>🌐 Total General (sin filtros)</p>",
        unsafe_allow_html=True
    )
    _fila_kpis(ind_global)

    st.markdown("<hr class='kpi-separator'>", unsafe_allow_html=True)

    # ── BLOQUE FILTRADO: Métricas aplicando filtros del usuario ──
    # Calcular diferencia de cumplimiento para mostrar variación vs global
    delta_pct = round(ind_filtrado['pct_cumplimiento'] - ind_global['pct_cumplimiento'], 1)
    delta_str = f"({'+' if delta_pct >= 0 else ''}{delta_pct}% vs global)"
    color_delta = "#22c55e" if delta_pct >= 0 else "#ef4444"  # Verde si mejora, rojo si empeora

    st.markdown(
        f"<p style='margin:0 0 6px 0; color:#8b9dc3; font-size:0.78rem; "
        f"text-transform:uppercase; letter-spacing:0.06em;'>"
        f"🔍 {etiqueta_filtro} "
        f"<span style='color:{color_delta}; font-weight:700'>{delta_str}</span></p>",
        unsafe_allow_html=True
    )
    _fila_kpis(ind_filtrado)
    
    # ── NUEVO: Fila de KPIs Financieros (solo si hay datos monetarios) ──
    if 'Valor despacho' in st.session_state.get('df_filtrado_actual', pd.DataFrame()).columns:
        st.markdown("<hr class='kpi-separator'>", unsafe_allow_html=True)
        _fila_kpis_financieros(st.session_state.df_filtrado_actual)


# ──────────────────────────────────────────────────────────────────────────
# 📈 GRÁFICOS INTERACTIVOS CON PLOTLY
# ──────────────────────────────────────────────────────────────────────────
def mostrar_graficos(processor, df_filtrado: pd.DataFrame, debug_mode: bool = False) -> None:
    """
    Renderiza todos los gráficos interactivos del dashboard en layout responsivo.
    
    Args:
        processor: Instancia de DataProcessor para métodos de análisis
        df_filtrado: DataFrame con datos filtrados por el usuario
        debug_mode: Flag para mostrar información de debugging en consola
    """
    # Guardar df_filtrado en session_state para acceso en KPIs financieros
    st.session_state.df_filtrado_actual = df_filtrado
    
    # ── FILA 1: Pie Chart Cumplimiento NNS + Barras Desvíos ──
    col1, col2 = st.columns(2)

    with col1:
        st.markdown("### 🎯 Cumplimiento NNS")
        # Contar frecuencias de cada categoría de cumplimiento
        counts = df_filtrado['Cumple_NNS'].value_counts().reset_index()
        counts.columns = ['Categoria', 'Cantidad']
        
        # Gráfico de dona con Plotly Express (más simple para este caso)
        fig = px.pie(
            counts, names='Categoria', values='Cantidad',
            hole=0.55,  # Agujero central para estilo dona
            color='Categoria',
            color_discrete_map={
                'Cumple': COLOR_CUMPLE, 
                'No cumple': COLOR_NO_CUMPLE, 
                'PTE': COLOR_PTE
            },
            template=PLOTLY_TEMPLATE,
            custom_data=['Categoria']  # Metadatos para drill-down al hacer click
        )
        fig.update_layout(margin=dict(l=20, r=20, t=20, b=20), showlegend=True)
        
        # Renderizar gráfico con interactividad: on_select="rerun" para capturar clicks
        sel_nns = st.plotly_chart(fig, use_container_width=True, on_select="rerun", key="chart_nns_v5")
        
        if debug_mode and sel_nns:
            st.write("🐛 Debug NNS Select:", sel_nns)

        # Drill-down: mostrar datos fuente al seleccionar una rodaja
        if sel_nns and 'selection' in sel_nns:
            mostrar_datos_fuente(df_filtrado, sel_nns['selection'], 
                                [('Cumple_NNS', 'Categoria')], 
                                titulo_seccion="🎯 Detalle de Pedidos por Cumplimiento")
        else:
            st.caption("💡 Haz clic en una rodaja para ver el detalle")

    with col2:
        st.markdown("### 📊 Desvíos en Despacho vs Entrega")
        ind = processor.get_indicadores(df_filtrado)
        total_e = ind['total_pedidos'] if ind else 0
        
        # Calcular valores para las 3 categorías de desvío
        categorias = ['Sin Desvío', 'Desvío Despacho', 'Desvío Entrega']
        valores = [
            max(0, total_e - (ind['con_desvio_despacho'] if ind else 0)),
            ind['con_desvio_despacho'] if ind else 0,
            ind['con_desvio_entrega'] if ind else 0,
        ]
        colores = [COLOR_CUMPLE, COLOR_PTE, COLOR_NO_CUMPLE]  # Semántica de colores
        
        # Gráfico de barras con Plotly Graph Objects (más control personalizado)
        fig2 = go.Figure(go.Bar(
            x=categorias, y=valores,
            marker_color=colores,
            text=valores, textposition='outside',  # Mostrar valores sobre barras
        ))
        fig2.update_layout(**fig_base(), yaxis_title='Pedidos', showlegend=False)
        st.plotly_chart(fig2, use_container_width=True)

    # ── FILA 2: Cumplimiento por Ciudad (Top 12) ──
    st.markdown("### 📍 Cumplimiento por Ciudad (Top 12)")
    analisis_c = processor.get_analisis_ciudad(df_filtrado)
    
    if analisis_c is not None and len(analisis_c) > 0:
        top_c = analisis_c.head(12).copy()  # Limitar a 12 ciudades para legibilidad
        
        fig3 = px.bar(
            top_c, x='Ciudad', y='Pct_Cumplimiento',
            color='Pct_Cumplimiento',  # Color por valor (heatmap vertical)
            color_continuous_scale=['#ef4444', '#f59e0b', '#22c55e'],  # Rojo→Ámbar→Verde
            text='Pct_Cumplimiento',
            custom_data=['Ciudad', 'Total'],  # Metadatos para drill-down
            template=PLOTLY_TEMPLATE,
            hover_data=['Total', 'No_Cumplen']  # Info adicional al pasar mouse
        )
        fig3.update_traces(texttemplate='%{text}%', textposition='outside')
        # Línea de referencia: meta del 95% de cumplimiento
        fig3.add_hline(y=95, line_dash='dash', line_color=COLOR_PTE,
                       annotation_text='Meta 95%', annotation_position='top left')
        fig3.update_layout(**fig_base(), yaxis_title='% Cumplimiento',
                           yaxis_range=[0, 115], coloraxis_showscale=False)
        
        sel_c = st.plotly_chart(fig3, use_container_width=True, on_select="rerun", key="chart_ciudad_v5")
        
        if debug_mode and sel_c:
            st.write("🐛 Debug Ciudad Select:", sel_c)

        # Drill-down por ciudad
        if sel_c and 'selection' in sel_c:
            mostrar_datos_fuente(df_filtrado, sel_c['selection'], 
                                [('Ciudad', 'Ciudad')], 
                                titulo_seccion="📍 Detalle de Pedidos por Ciudad")
        else:
            st.caption("💡 Haz clic en una barra para ver detalle")

    # ── FILA 3: Transportadora + Área Responsable ──
    col3, col4 = st.columns(2)

    with col3:
        st.markdown("### 🚚 Desempeño por Transportadora")
        analisis_t = processor.get_analisis_transportadora(df_filtrado)
        
        if analisis_t is not None and len(analisis_t) > 0:
            top_t = analisis_t.head(8).copy()  # Top 8 transportadoras
            
            fig4 = px.bar(
                top_t,
                x='Transportadora', y='Pct_Cumplimiento',
                color='Desvio_Prom',  # Color por desvío promedio (otra dimensión)
                color_continuous_scale=['#22c55e', '#f59e0b', '#ef4444'],
                text='Pct_Cumplimiento',
                custom_data=['Transportadora'],
                template=PLOTLY_TEMPLATE,
            )
            fig4.update_traces(
                texttemplate='%{text:.1f}%',  # Formato con 1 decimal
                textposition='outside',
                hovertemplate='<b>%{x}</b><br>Cumplimiento: %{y:.1f}%<br>'
                              'Desvío prom: %{customdata[0]:.1f}d<extra></extra>'
            )
            fig4.update_layout(**fig_base(), yaxis_title='% Cumplimiento',
                               yaxis_range=[0, 115], coloraxis_showscale=False)
            fig4.add_hline(y=95, line_dash='dash', line_color=COLOR_PTE)
            
            sel_t = st.plotly_chart(fig4, use_container_width=True, on_select="rerun", key="chart_transp_v5")
            
            if debug_mode and sel_t:
                st.write("🐛 Debug Transp Select:", sel_t)

            # Drill-down por transportadora
            if sel_t and 'selection' in sel_t:
                mostrar_datos_fuente(df_filtrado, sel_t['selection'], 
                                    [('Transportadora', 'Transportadora')], 
                                    titulo_seccion="🚚 Detalle de Pedidos por Transportadora")
            else:
                st.caption("💡 Haz clic en una barra para ver detalle")

    with col4:
        st.markdown("### 🏢 Responsabilidad del Incumplimiento")
        inc = processor.get_pedidos_incumplimiento(df_filtrado)
        
        if inc is not None and len(inc) > 0 and 'Area_Incumple' in inc.columns:
            areas = inc['Area_Incumple'].value_counts().reset_index()
            areas.columns = ['Area', 'Cantidad']
            
            fig5 = px.pie(
                areas, names='Area', values='Cantidad',
                hole=0.45, template=PLOTLY_TEMPLATE,
                custom_data=['Area']
            )
            fig5.update_layout(**fig_base())
            
            sel_a = st.plotly_chart(fig5, use_container_width=True, on_select="rerun", key="chart_area_v5")
            
            if debug_mode and sel_a:
                st.write("🐛 Debug Area Select:", sel_a)

            # Drill-down por área responsable
            if sel_a and 'selection' in sel_a:
                mostrar_datos_fuente(df_filtrado, sel_a['selection'], 
                                    [('Area_Incumple', 'Area')], 
                                    titulo_seccion="🏢 Detalle de Responsabilidad")
            else:
                st.caption("💡 Haz clic para ver detalle del área")
        else:
            st.success("🎉 Sin incumplimientos en el período seleccionado.")

    # ── FILA 4: Tendencia Mensual (Gráfico Combinado Barras + Línea) ──
    st.markdown("### 📈 Evolución Mensual del Cumplimiento NNS")
    analisis_m = processor.get_analisis_mes(df_filtrado)
    
    if analisis_m is not None and len(analisis_m) > 0:
        fig6 = go.Figure()
        
        # Serie 1: Barras con total de pedidos por mes (eje Y secundario)
        fig6.add_trace(go.Bar(
            x=analisis_m['Mes_Label'], y=analisis_m['Total'],
            name='Total pedidos',
            marker_color='#3b4a6b', opacity=0.7,
            yaxis='y2',  # Asignar a eje secundario
        ))
        
        # Serie 2: Línea con % de cumplimiento (eje Y principal)
        fig6.add_trace(go.Scatter(
            x=analisis_m['Mes_Label'], y=analisis_m['Pct_Cumplimiento'],
            name='% Cumplimiento NNS',
            line=dict(color=COLOR_PRIMARY, width=3),
            mode='lines+markers+text',  # Línea + puntos + etiquetas de texto
            text=[f"{v}%" for v in analisis_m['Pct_Cumplimiento']],
            textposition='top center',
            textfont=dict(size=10, color='#a5b4fc'),
        ))
        
        # Línea de referencia: meta del 95%
        fig6.add_hline(y=95, line_dash='dash', line_color=COLOR_PTE,
                       annotation_text='Meta 95%', annotation_position='bottom right')
        
        # Configurar layout con dos ejes Y superpuestos
        layout = fig_base()
        layout.update({
            'yaxis': {'title': '% Cumplimiento', 'range': [0, 115], 'side': 'left'},
            'yaxis2': {'title': 'Total Pedidos', 'overlaying': 'y', 'side': 'right', 'showgrid': False},
            'legend': {'orientation': 'h', 'y': -0.15},  # Leyenda horizontal abajo
        })
        fig6.update_layout(**layout)
        st.plotly_chart(fig6, use_container_width=True)
    else:
        st.info("ℹ️ Selecciona más de un mes para ver la tendencia temporal.")

    # ── NUEVO: FILA 5 - Análisis de Causas Raíz (Pareto) ──
    st.markdown("### 🎯 Análisis de Causas Raíz (Principio de Pareto)")
    
    if 'Causal de Incumplimiento' in df_filtrado.columns:
        # Filtrar solo pedidos que NO cumplieron para análisis de causas
        df_inc = df_filtrado[df_filtrado['Cumple_NNS'] == 'No cumple'].copy()
        
        if len(df_inc) > 0:
            # Agrupar y contar frecuencias de cada causa
            causas = df_inc['Causal de Incumplimiento'].value_counts().reset_index()
            causas.columns = ['Causal', 'Frecuencia']
            causas['Porcentaje'] = (causas['Frecuencia'] / causas['Frecuencia'].sum() * 100).round(1)
            causas['Porcentaje Acum'] = causas['Porcentaje'].cumsum()  # Acumulado para curva Pareto
            
            # Crear gráfico combinado: barras (frecuencia) + línea (% acumulado)
            fig_pareto = go.Figure()
            fig_pareto.add_trace(go.Bar(
                x=causas['Causal'], y=causas['Frecuencia'],
                name='Frecuencia', marker_color=COLOR_NO_CUMPLE,
                text=causas['Frecuencia'], textposition='outside'
            ))
            fig_pareto.add_trace(go.Scatter(
                x=causas['Causal'], y=causas['Porcentaje Acum'],
                name='% Acumulado', line=dict(color=COLOR_PRIMARY, width=3),
                mode='lines+markers+text',
                text=[f"{v}%" for v in causas['Porcentaje Acum']],
                textposition='top center',
                yaxis='y2'  # Eje secundario para porcentaje acumulado
            ))
            
            fig_pareto.update_layout(
                **fig_base(),
                title='Principales Causas de Incumplimiento',
                yaxis=dict(title='Frecuencia', side='left'),
                yaxis2=dict(title='% Acumulado', overlaying='y', side='right', range=[0, 110]),
                annotations=[dict(
                    x=0.5, y=95, xref='paper', yref='y2',
                    text='Meta 95%', showarrow=True, arrowhead=2,
                    ax=0, ay=-40, font=dict(color=COLOR_PTE)
                )],
                xaxis_tickangle=-45  # Rotar etiquetas X para mejor legibilidad
            )
            st.plotly_chart(fig_pareto, use_container_width=True)
            
            # Insight automático basado en la causa principal
            top_causa = causas.iloc[0]
            st.caption(
                f"💡 **Insight**: '{top_causa['Causal']}' representa el {top_causa['Porcentaje']}% "
                f"de los incumplimientos. Enfocar mejoras aquí podría resolver "
                f"{causas['Porcentaje Acum'].iloc[0]:.0f}% del problema."
            )
        else:
            st.success("🎉 Sin incumplimientos para analizar causas en el período seleccionado.")
    else:
        st.info("ℹ️ Columna 'Causal de Incumplimiento' no disponible en los datos.")

    # ── NUEVO: FILA 6 - Análisis por Categoría de Producto ──
    if 'Categoria' in df_filtrado.columns:
        st.markdown("### 📦 Desempeño por Categoría de Producto")
        
        # Agrupar datos por categoría con múltiples métricas
        
        
        # Columna correcta para contar pedidos
    col_contar = 'No_Orden' if 'No_Orden' in df_filtrado.columns else 'No orden'

    analisis_cat = df_filtrado.groupby('Categoria').agg({
        col_contar: 'count',  # ← Usar columna válida
        'Cumple_NNS': lambda x: (x == 'Cumple').sum() / len(x) * 100,
        'Desvio_Entrega': 'mean'
    }).round(2).reset_index()

    # Agregar valor total si existe Valor_num
    if 'Valor_num' in df_filtrado.columns:
        valor_total = df_filtrado.groupby('Categoria')['Valor_num'].sum().reset_index()
        valor_total.columns = ['Categoria', 'Valor Total']
        analisis_cat = analisis_cat.merge(valor_total, on='Categoria', how='left')
    else:
        analisis_cat['Valor Total'] = 0

    # Renombrar columnas para claridad
    analisis_cat.columns = ['Categoria', 'Pedidos', '% Cumplimiento', 'Desvío Prom'] + (['Valor Total'] if 'Valor Total' in analisis_cat.columns else [])
    analisis_cat = analisis_cat.sort_values('Pedidos', ascending=False)

    # Gráfico de burbujas: X=% cumplimiento, Y=Valor total, tamaño=# pedidos    
        # Renombrar columnas para claridad
    col_valor = 'Valor_num' if 'Valor_num' in df_filtrado.columns else 'No orden'
    analisis_cat.columns = ['Categoria', 'Pedidos', '% Cumplimiento', 'Valor Total', 'Desvío Prom']
    analisis_cat = analisis_cat.sort_values('Valor Total', ascending=False)
        
        # Gráfico de burbujas: X=% cumplimiento, Y=Valor total, tamaño=# pedidos
    fig_cat = px.scatter(
            analisis_cat,
            x='% Cumplimiento', y='Valor Total',
            size='Pedidos', color='Categoria',
            hover_data=['Desvío Prom'],
            text='Categoria',
            color_discrete_sequence=px.colors.qualitative.Set2,  # Paleta de colores distintivos
            template=PLOTLY_TEMPLATE
        )
    fig_cat.update_traces(textposition='top center', marker=dict(sizemode='diameter'))
    fig_cat.update_layout(**fig_base(), yaxis_title='Valor Total Despachos (COP)')
        
        # Línea de referencia: meta de 95% cumplimiento
    fig_cat.add_vline(x=80, line_dash='dash', line_color=COLOR_PTE, annotation_text='Meta 95%')
        
    st.plotly_chart(fig_cat, use_container_width=True)
        
        # Tabla interactiva con formato personalizado
    st.dataframe(
            analisis_cat.style.format({
                'Valor Total': '${:,.0f}',
                '% Cumplimiento': '{:.1f}%',
                'Desvío Prom': '{:.1f} días'
            }), 
            use_container_width=True
        )


# ──────────────────────────────────────────────────────────────────────────
# 🚨 SISTEMA DE ALERTAS PROACTIVAS (NUEVA FUNCIONALIDAD)
# ──────────────────────────────────────────────────────────────────────────
def generar_alertas(df_filtrado: pd.DataFrame, ind_filtrado: Dict) -> List[Dict]:
    """
    Genera alertas automáticas basadas en umbrales configurables de negocio.
    
    Args:
        df_filtrado: DataFrame con datos filtrados para análisis
        ind_filtrado: Diccionario con indicadores calculados
        
    Returns:
        Lista de dicts con estructura: {'tipo', 'titulo', 'mensaje'}
    """
    alertas = []
    
    # 🔴 Alerta 1: Cumplimiento general por debajo del mínimo aceptable
    if ind_filtrado.get('pct_cumplimiento', 100) < UMBRALES_ALERTAS['cumplimiento_minimo']:
        alertas.append({
            'tipo': 'error',
            'titulo': '🔴 Cumplimiento Crítico',
            'mensaje': (
                f"El cumplimiento NNS está en {ind_filtrado['pct_cumplimiento']}% "
                f"(meta: {UMBRALES_ALERTAS['cumplimiento_minimo']}%). "
                f"Revisar procesos de producción y logística de inmediato."
            )
        })
    
    # ⚠️ Alerta 2: Desvío promedio de entrega elevado
    if ind_filtrado.get('promedio_desvio_entrega', 0) > UMBRALES_ALERTAS['desvio_entrega_max']:
        alertas.append({
            'tipo': 'warning',
            'titulo': '⚠️ Desvíos Elevados',
            'mensaje': (
                f"El desvío promedio de entrega es de {ind_filtrado['promedio_desvio_entrega']} días "
                f"(límite: {UMBRALES_ALERTAS['desvio_entrega_max']} días). "
                f"Evaluar capacidad de transporte y planificación de rutas."
            )
        })
    
    # ⚠️ Alerta 3: Transportadoras con bajo desempeño (si hay datos)
    if 'Transportadora' in df_filtrado.columns and len(df_filtrado) > 0:
        # Calcular % cumplimiento por transportadora
        perf_transp = df_filtrado.groupby('Transportadora')['Cumple_NNS'].apply(
            lambda x: (x == 'Cumple').sum() / len(x) * 100 if len(x) > 0 else 0
        )
        # Identificar transportadoras por debajo del umbral mínimo
        malas = perf_transp[perf_transp < UMBRALES_ALERTAS['transportadora_min_perf']]
        if len(malas) > 0:
            alertas.append({
                'tipo': 'warning',
                'titulo': '🚚 Transportadoras con Bajo Desempeño',
                'mensaje': (
                    f"{', '.join(malas.index.tolist())} tienen cumplimiento <"
                    f"{UMBRALES_ALERTAS['transportadora_min_perf']}%. "
                    f"Considerar reevaluación de contratos o capacitación."
                )
            })
    
    # ℹ️ Alerta 4: Pedidos pendientes (PTE) acumulados
    if 'Cumple_NNS' in df_filtrado.columns and 'PTE' in df_filtrado['Cumple_NNS'].values:
        ptes = df_filtrado[df_filtrado['Cumple_NNS'] == 'PTE']
        if len(ptes) > UMBRALES_ALERTAS['pendientes_max']:
            alertas.append({
                'tipo': 'info',
                'titulo': '⏳ Pendientes Acumulados',
                'mensaje': (
                    f"{len(ptes)} pedidos sin fecha de entrega registrada "
                    f"(umbral: {UMBRALES_ALERTAS['pendientes_max']}). "
                    f"Verificar estado en sistema y actualizar trazabilidad."
                )
            })
    
    return alertas


def mostrar_alertas(alertas: List[Dict]) -> None:
    """
    Renderiza visualmente las alertas generadas usando tarjetas con colores semánticos.
    
    Args:
        alertas: Lista de dicts con estructura de alerta {'tipo', 'titulo', 'mensaje'}
    """
    if not alertas:
        st.success("✅ No hay alertas activas. Los indicadores están dentro de parámetros normales.")
        return
    
    # Mapeo de tipo de alerta a clase CSS para estilo visual
    css_class = {
        'success': 'rec-success', 
        'warning': 'rec-warning',
        'error': 'rec-error', 
        'info': 'rec-info'
    }

    for alerta in alertas:
        cls = css_class.get(alerta['tipo'], 'rec-info')  # Fallback a info si tipo desconocido
        st.markdown(
            f'<div class="rec-card {cls}">'
            f'<strong style="color:#e8ecf4">{alerta["titulo"]}</strong><br>'
            f'<span style="color:#8b9dc3;font-size:0.9rem">{alerta["mensaje"]}</span>'
            f'</div>',
            unsafe_allow_html=True
        )


# ──────────────────────────────────────────────────────────────────────────
# 💡 RECOMENDACIONES AUTOMATIZADAS (Basadas en análisis de datos)
# ──────────────────────────────────────────────────────────────────────────
def mostrar_recomendaciones(processor, df_filtrado: pd.DataFrame) -> None:
    """
    Muestra recomendaciones generadas automáticamente por DataProcessor.
    
    Args:
        processor: Instancia de DataProcessor con métodos de análisis
        df_filtrado: DataFrame con datos filtrados para contexto
    """
    st.markdown("### 💡 Análisis de Mejora")
    recs = processor.get_recomendaciones(df_filtrado)

    if not recs:
        st.info("ℹ️ No hay suficientes datos para generar recomendaciones automatizadas.")
        return

    # Mapeo de tipo de recomendación a clase CSS para estilo visual
    css_class = {
        'success': 'rec-success', 
        'warning': 'rec-warning',
        'error': 'rec-error', 
        'info': 'rec-info'
    }

    for titulo, cuerpo, tipo in recs:
        cls = css_class.get(tipo, 'rec-info')
        st.markdown(
            f'<div class="rec-card {cls}">'
            f'<strong style="color:#e8ecf4">{titulo}</strong><br>'
            f'<span style="color:#8b9dc3;font-size:0.9rem">{cuerpo}</span>'
            f'</div>',
            unsafe_allow_html=True
        )


# ──────────────────────────────────────────────────────────────────────────
# 📋 TABLA DE DETALLE CON SUB-FILTROS Y EXPORTACIÓN
# ──────────────────────────────────────────────────────────────────────────
def mostrar_tabla_detalle(processor, df_filtrado: pd.DataFrame) -> None:
    """
    Muestra tabla interactiva de incumplimientos con filtros adicionales y exportación.
    
    Args:
        processor: Instancia de DataProcessor
        df_filtrado: DataFrame con datos filtrados globales
    """
    st.markdown("### 📋 Detalle de Incumplimientos")

    inc = processor.get_pedidos_incumplimiento(df_filtrado)

    if inc is None or len(inc) == 0:
        st.success("🎉 No hay pedidos con incumplimiento en el período seleccionado.")
        return

    # ── SUB-FILTROS ESPECÍFICOS PARA LA TABLA DE INCUMPLIMIENTOS ──
    cf1, cf2, cf3 = st.columns(3)
    
    with cf1:
        ciudades_inc = ['Todas'] + sorted(inc['Ciudad'].dropna().astype(str).unique().tolist())
        c_sel = st.selectbox("📍 Ciudad", ciudades_inc, key='tab_ciudad', index=0)
    
    with cf2:
        if 'Area_Incumple' in inc.columns:
            areas_inc = ['Todas'] + sorted(inc['Area_Incumple'].dropna().unique().tolist())
            a_sel = st.selectbox("🏢 Área Responsable", areas_inc, key='tab_area', index=0)
        else:
            a_sel = 'Todas'
    
    with cf3:
        if 'Desvio_Entrega' in inc.columns and len(inc['Desvio_Entrega'].dropna()) > 0:
            min_d = float(inc['Desvio_Entrega'].min())
            max_d = float(inc['Desvio_Entrega'].max())
            d_sel = st.slider("⏱️ Desvío mínimo (días)", min_value=min_d, max_value=max_d,
                              value=min_d, key='tab_desvio')
        else:
            d_sel = 0

    # Aplicar sub-filtros a la tabla de incumplimientos
    df_t = inc.copy()
    if c_sel != 'Todas':
        df_t = df_t[df_t['Ciudad'].astype(str) == c_sel]
    if a_sel != 'Todas' and 'Area_Incumple' in df_t.columns:
        df_t = df_t[df_t['Area_Incumple'] == a_sel]
    if 'Desvio_Entrega' in df_t.columns:
        df_t = df_t[df_t['Desvio_Entrega'] >= d_sel]

    st.caption(f"Mostrando {len(df_t):,} de {len(inc):,} incumplimientos")

    # ── RENOMBRAR COLUMNAS PARA DISPLAY AMIGABLE ──
    rename_display = {
        'Fecha': 'Fecha Compra', 'No_Orden': 'No. Orden',
        'Cliente': 'Cliente', 'Producto': 'Producto',
        'Ciudad': 'Ciudad', 'Transportadora': 'Transportadora',
        'No_Guia': 'No. Guía', 'Fecha_Despacho': 'F. Despacho',
        'Fecha_Entrega': 'F. Entrega',
        'Dias_Despacho_Hab': 'Días Despacho', 'Dias_Entrega_Hab': 'Días Entrega',
        'SLA_Entrega': 'SLA', 'Desvio_Despacho': 'Desvío Despacho',
        'Desvio_Entrega': 'Desvío Entrega', 'Area_Incumple': 'Área Responsable',
        'Valor despacho': 'Valor Despacho', 'Causal de Incumplimiento': 'Causal',
        'Categoria': 'Categoría', 'Concepto': 'Tipo'
    }
    df_show = df_t.rename(columns={k: v for k, v in rename_display.items() if k in df_t.columns})

    # Tabla interactiva con scroll horizontal si hay muchas columnas
    st.dataframe(df_show, use_container_width=True, hide_index=True)

    # ── BOTÓN DE EXPORTACIÓN A EXCEL ──
    col_exp1, col_exp2 = st.columns([1, 4])
    with col_exp1:
        try:
            buf = io.BytesIO()
            df_t.to_excel(buf, index=False, sheet_name='Incumplimientos')
            buf.seek(0)
            st.download_button(
                "📥 Exportar a Excel",
                data=buf,
                file_name="incumplimientos_filtrados.xlsx",
                mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
                help="Descarga los incumplimientos filtrados en formato Excel"
            )
        except Exception as e:
            logger.error(f"Error exportando tabla: {e}")
            st.error("❌ Error al generar archivo de exportación")


# ──────────────────────────────────────────────────────────────────────────
# 📤 EXPORTACIÓN AVANZADA: MEGA REPORTE CON MÚLTIPLES HOJAS
# ──────────────────────────────────────────────────────────────────────────
def generate_report_advanced(
    df_filtrado: pd.DataFrame, 
    ind_filtrado: Dict, 
    ind_global: Dict,
    processor
) -> io.BytesIO:
    """
    Genera archivo Excel con múltiples hojas: resumen, datos, análisis por categoría y causales.
    
    Args:
        df_filtrado: DataFrame con datos filtrados
        ind_filtrado: Indicadores del subset filtrado
        ind_global: Indicadores del dataset completo
        processor: Instancia de DataProcessor para métodos adicionales
        
    Returns:
        BytesIO con archivo Excel en memoria listo para descarga
    """
    buf = io.BytesIO()
    
    with pd.ExcelWriter(buf, engine='openpyxl') as writer:
        # ── HOJA 1: RESUMEN EJECUTIVO ──
        resumen = pd.DataFrame({
            'Métrica': [
                'Total Pedidos', 'Cumplimiento NNS', 'Desvío Promedio Entrega', 
                'Valor Total Despachos', 'Transportadoras Activas', 'Ciudades Atendidas'
            ],
            'Valor': [
                ind_filtrado['total_pedidos'],
                f"{ind_filtrado['pct_cumplimiento']}%",
                f"{ind_filtrado['promedio_desvio_entrega']} días",
                f"${df_filtrado.get('Valor_num', pd.Series([0])).sum():,.0f}" if 'Valor_num' in df_filtrado.columns else 'N/A',
                df_filtrado['Transportadora'].nunique() if 'Transportadora' in df_filtrado.columns else 0,
                df_filtrado['Ciudad'].nunique() if 'Ciudad' in df_filtrado.columns else 0
            ],
            'Variación vs Global': [
                f"{ind_filtrado['total_pedidos'] - ind_global['total_pedidos']:+d}",
                f"{ind_filtrado['pct_cumplimiento'] - ind_global['pct_cumplimiento']:+.1f}%",
                '-', '-', '-', '-'
            ]
        })
        resumen.to_excel(writer, sheet_name='📊 Resumen Ejecutivo', index=False)
        
        # ── HOJA 2: DATOS FILTRADOS COMPLETOS ──
        df_filtrado.to_excel(writer, sheet_name='📋 Datos Filtrados', index=False)
        
        # ── HOJA 3: ANÁLISIS POR CATEGORÍA (si aplica) ──
        if 'Categoria' in df_filtrado.columns:
            cat_analysis = df_filtrado.groupby('Categoria').agg({
                'No orden': 'count',
                'Cumple_NNS': lambda x: (x == 'Cumple').sum() / len(x) * 100,
                'Valor_num' if 'Valor_num' in df_filtrado.columns else 'No orden': 'sum'
            }).round(2).reset_index()
            cat_analysis.columns = ['Categoria', 'Pedidos', '% Cumplimiento', 'Valor Total']
            cat_analysis.to_excel(writer, sheet_name='📦 Por Categoría', index=False)
        
        # ── HOJA 4: CAUSALES DE INCUMPLIMIENTO (si aplica) ──
        if 'Causal de Incumplimiento' in df_filtrado.columns:
            df_inc = df_filtrado[df_filtrado['Cumple_NNS'] == 'No cumple']
            if len(df_inc) > 0:
                causal_analysis = df_inc['Causal de Incumplimiento'].value_counts().reset_index()
                causal_analysis.columns = ['Causal', 'Frecuencia']
                causal_analysis.to_excel(writer, sheet_name='🎯 Causales', index=False)
        
        # ── APLICAR FORMATO PROFESIONAL A LAS HOJAS ──
        workbook = writer.book
        for sheet_name in workbook.sheetnames:
            worksheet = workbook[sheet_name]
            # Estilo para fila de encabezado: fondo índigo, texto blanco, negrita, centrado
            for cell in worksheet[1]:
                cell.font = Font(bold=True, color="FFFFFF")
                cell.fill = PatternFill(start_color="4F46E5", end_color="4F46E5", fill_type="solid")
                cell.alignment = Alignment(horizontal="center")
    
    buf.seek(0)  # Reiniciar buffer para lectura
    return buf


# ──────────────────────────────────────────────────────────────────────────
# 🚀 FUNCIÓN PRINCIPAL: ORQUESTACIÓN DEL DASHBOARD
# ──────────────────────────────────────────────────────────────────────────
def main():
    """
    Función principal que orquesta todo el flujo de la aplicación Streamlit.
    Sigue el patrón: Carga → Procesamiento → Filtrado → Visualización → Exportación
    """
    # ── SIDEBAR: CARGA DE ARCHIVO EXCEL ──
    st.sidebar.markdown("### 📂 Cargar Archivo")
    uploaded = st.sidebar.file_uploader(
        "Archivo Excel (.xlsx / .xls)",
        type=['xlsx', 'xls'],
        help="Sube el archivo de Seguimiento de Despachos TECU con las columnas esperadas"
    )

    # ── PANTALLA DE BIENVENIDA (si no hay archivo cargado) ──
    if uploaded is None:
        st.markdown("# 📦 TECU – Análisis de Despachos")
        st.markdown("---")

        col_i1, col_i2, col_i3 = st.columns(3)
        with col_i1:
            st.info("**📅 SLA por ciudad**\n\n"
                    "- **3 días hábiles** → Bogotá, Medellín, Cali\n"
                    "- **5 días hábiles** → Otras ciudades")
        with col_i2:
            st.info("**📊 Métricas calculadas**\n\n"
                    "- % Cumplimiento NNS\n"
                    "- Desvíos despacho & entrega\n"
                    "- Área responsable\n"
                    "- **NUEVO**: KPIs financieros y análisis por categoría")
        with col_i3:
            st.info("**🔍 Filtros disponibles**\n\n"
                    "- Por Mes, Transportadora, Ciudad\n"
                    "- **NUEVO**: Categoría, Concepto, Rango de Valor\n"
                    "- Drill-down interactivo en gráficos")

        st.markdown("\n#### 👆 Sube el archivo Excel en el panel izquierdo para comenzar.")
        st.markdown(
            "> 💡 **Tip**: Los datos se procesan localmente en tu navegador. "
            "Ninguna información sale de tu computadora."
        )
        return

    # ── SIDEBAR: CONFIGURACIÓN DE PARÁMETROS SLA ──
    st.sidebar.markdown("### ⚙️ Configuración SLA")
    sl_alm = st.sidebar.slider(
        "Límite Almacén (días)", 1, 5, 1, 
        help="Días hábiles máximos permitidos para despacho desde almacén"
    )
    sl_pri = st.sidebar.slider(
        "SLA Ciudades Principales (días)", 1, 3, 3, 
        help="Tiempo máximo de entrega para Bogotá, Medellín, Cali"
    )
    sl_otr = st.sidebar.slider(
        "SLA Otras Ciudades (días)", 3, 5, 5,
        help="Tiempo máximo de entrega para el resto de destinos"
    )
    st.sidebar.markdown("---")

    # ── PROCESAMIENTO DE DATOS CON INDICADOR DE CARGA ──
    with st.spinner("⏳ Procesando datos..."):
        logger.info(f"Iniciando procesamiento con SLA: almacén={sl_alm}, principal={sl_pri}, otras={sl_otr}")
        processor, df_procesado, hoja = cargar_y_procesar(uploaded, sl_alm, sl_pri, sl_otr)

    # Validar que el procesamiento fue exitoso
    if processor is None or df_procesado is None:
        st.error("❌ No se pudo procesar el archivo. Verifica que tenga el formato esperado.")
        logger.error("Fallo en procesamiento de archivo subido")
        return

    # ── APLICAR FILTROS GLOBALES DEL SIDEBAR ──
    df_filtrado, debug_mode = sidebar_filtros(df_procesado)
    logger.info(f"Filtros aplicados: {len(df_filtrado)} registros de {len(df_procesado)} totales")

    # ── BOTÓN DE EXPORTACIÓN AVANZADA (MEGA REPORTE) ──
    st.sidebar.markdown("---")
    st.sidebar.markdown("### 📊 Reportes")
    try:
        ind_global = processor.get_indicadores(df_procesado)
        ind_filtrado = processor.get_indicadores(df_filtrado)
        
        if ind_filtrado:
            # Texto dinámico del botón según si hay filtros activos
            btn_label = "📥 Descargar Mega Reporte"
            if len(df_filtrado) < len(df_procesado):
                btn_label = "📥 Descargar Reporte Filtrado"
                
            # Generar archivo Excel con múltiples hojas de análisis
            mega_buf = generate_report_advanced(df_filtrado, ind_filtrado, ind_global, processor)
            st.sidebar.download_button(
                btn_label,
                data=mega_buf,
                file_name=f"Reporte_TECU_Analisis_{datetime.now().strftime('%Y%m%d_%H%M')}.xlsx",
                mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
                help="Excel con: Resumen Ejecutivo, Datos Filtrados, Análisis por Categoría y Causales"
            )
    except Exception as e:
        logger.error(f"Error generando reporte avanzado: {e}", exc_info=True)
        st.sidebar.error(f"⚠️ Error generando reporte: {e}")

    # ── HEADER PRINCIPAL DEL DASHBOARD ──
    st.markdown("# 📊 Dashboard de Despachos TECU Aura `v2.0` 🚀")
    
    # Badge informativo si hay filtros activos
    if len(df_filtrado) < len(df_procesado):
        st.info(f"💡 Filtro Activo: Viendo {len(df_filtrado)} de {len(df_procesado)} registros.")
    
    # Metadatos del archivo y selección actual
    st.caption(
        f"**Archivo:** `{uploaded.name}` &nbsp;|&nbsp; "
        f"**Hoja:** `{hoja}` &nbsp;|&nbsp; "
        f"**Registros seleccionados:** {len(df_filtrado):,} / {len(df_procesado):,}"
    )
    st.markdown("---")

    # ── CÁLCULO DE INDICADORES (GLOBAL Y FILTRADO) ──
    ind_global = processor.get_indicadores(df_procesado)
    indicadores = processor.get_indicadores(df_filtrado)

    # Validar que hay datos para mostrar
    if indicadores is None or indicadores['total_pedidos'] == 0:
        st.warning("⚠️ No hay pedidos con status 'Entregado' en el rango seleccionado.")
        st.dataframe(df_filtrado.head(20), use_container_width=True)
        return

    # ── RENDERIZAR KPIs COMPARATIVOS ──
    es_global = len(df_filtrado) == len(df_procesado)
    etiqueta = "Total General con filtros" if es_global else "Selección Actual"
    
    mostrar_kpis(ind_global, indicadores, etiqueta)
    st.markdown("---")

    # ── RENDERIZAR GRÁFICOS INTERACTIVOS ──
    mostrar_graficos(processor, df_filtrado, debug_mode)
    st.markdown("---")

    # ── RENDERIZAR SISTEMA DE ALERTAS PROACTIVAS (NUEVO) ──
    st.markdown("### 🚨 Alertas Automáticas")
    alertas = generar_alertas(df_filtrado, indicadores)
    mostrar_alertas(alertas)
    st.markdown("---")

    # ── RENDERIZAR RECOMENDACIONES AUTOMATIZADAS ──
    mostrar_recomendaciones(processor, df_filtrado)
    st.markdown("---")

    # ── RENDERIZAR TABLA DE DETALLE CON SUB-FILTROS ──
    mostrar_tabla_detalle(processor, df_filtrado)


# ──────────────────────────────────────────────────────────────────────────
# 🏁 PUNTO DE ENTRADA DE LA APLICACIÓN
# ──────────────────────────────────────────────────────────────────────────
if __name__ == "__main__":
    # Crear carpeta de logs si no existe (para el sistema de logging)
    import os
    os.makedirs("logs", exist_ok=True)
    
    logger.info("🚀 Aplicación TECU Dashboard iniciada")
    main()