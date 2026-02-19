"""
Dashboard de Análisis de Despachos TECU Aura
Versión mejorada con filtros globales y análisis completo
"""

import streamlit as st
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
from data_processor import DataProcessor

# ─────────────────────────────────────────────────────────────────────────────
# Configuración
# ─────────────────────────────────────────────────────────────────────────────
st.set_page_config(
    page_title="TECU – Análisis de Despachos",
    page_icon="📦",
    layout="wide",
    initial_sidebar_state="expanded",
)

# CSS personalizado
st.markdown("""
<style>
    /* Fondo general */
    .stApp { background-color: #0f1117; }

    /* KPI cards */
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
    /* Separador entre bloques KPI */
    .kpi-separator {
        border: none;
        border-top: 1px dashed #2e3250;
        margin: 10px 0 14px 0;
    }

    /* Sidebar */
    [data-testid="stSidebar"] {
        background: linear-gradient(180deg, #111827, #1a2035);
        border-right: 1px solid #2e3250;
    }
    [data-testid="stSidebar"] h1, [data-testid="stSidebar"] h2,
    [data-testid="stSidebar"] h3 { color: #a5b4fc; }

    /* Sección titulos */
    h1 { color: #e8ecf4 !important; }
    h2, h3 { color: #a5b4fc !important; }

    /* Divider */
    hr { border-color: #2e3250; }

    /* Recomendaciones */
    .rec-card {
        background: #1a2035;
        border-left: 4px solid #6366f1;
        border-radius: 8px;
        padding: 12px 16px;
        margin-bottom: 12px;
    }
    .rec-success { border-left-color: #22c55e; }
    .rec-warning { border-left-color: #f59e0b; }
    .rec-error   { border-left-color: #ef4444; }
    .rec-info    { border-left-color: #38bdf8; }

    /* Tabla */
    .dataframe thead th { background: #252840 !important; }
</style>
""", unsafe_allow_html=True)

# ─────────────────────────────────────────────────────────────────────────────
# Constantes de colores
# ─────────────────────────────────────────────────────────────────────────────
COLOR_CUMPLE    = '#22c55e'
COLOR_NO_CUMPLE = '#ef4444'
COLOR_PTE       = '#f59e0b'
PLOTLY_TEMPLATE = 'plotly_dark'


# ─────────────────────────────────────────────────────────────────────────────
# Carga de datos
# ─────────────────────────────────────────────────────────────────────────────

@st.cache_data(show_spinner=False, ttl=3600)
def _cargar_df_nuclear_v7(archivo_bytes, nombre_archivo):
    """Busting cache and ensuring fresh data processing."""
    import io
    try:
        xl = pd.ExcelFile(io.BytesIO(archivo_bytes))

        # Buscar hoja correcta
        hoja = None
        for h in xl.sheet_names:
            if 'venta' in h.lower() or 'base' in h.lower() or 'despacho' in h.lower():
                hoja = h
                break
        if hoja is None:
            hoja = xl.sheet_names[0]

        # Detectar fila de encabezado
        df_raw = pd.read_excel(io.BytesIO(archivo_bytes), sheet_name=hoja, header=None, nrows=10)
        header_row = 0
        for i in range(len(df_raw)):
            row_vals = ' '.join([str(v).lower() for v in df_raw.iloc[i].values])
            if 'fecha' in row_vals or 'cliente' in row_vals or 'ciudad' in row_vals:
                header_row = i
                break

        df = pd.read_excel(io.BytesIO(archivo_bytes), sheet_name=hoja, header=header_row)

        # Procesar y devolver solo el DataFrame
        from data_processor import DataProcessor as _DP
        p = _DP(df)
        df_procesado = p.procesar()
        return df_procesado, hoja

    except Exception as e:
        st.error(f"Error al cargar: {e}")
        return None, None


def cargar_y_procesar(uploaded_file):
    """Wrapper que usa getvalue() (seguro en reruns) y crea un processor fresco."""
    archivo_bytes = uploaded_file.getvalue() 
    df_procesado, hoja = _cargar_df_nuclear_v7(archivo_bytes, uploaded_file.name)
    if df_procesado is None:
        return None, None, None
    # Crear processor FRESCO con df ya procesado (no llama a procesar() de nuevo)
    processor = DataProcessor(df_procesado)
    processor.df_procesado = df_procesado      # inyectar df directamente
    return processor, df_procesado, hoja


# ─────────────────────────────────────────────────────────────────────────────
# Helpers
# ─────────────────────────────────────────────────────────────────────────────
def color_tipo(tipo):
    return {
        'success': COLOR_CUMPLE,
        'warning': COLOR_PTE,
        'error':   COLOR_NO_CUMPLE,
        'info':    '#38bdf8',
    }.get(tipo, '#6366f1')


def fig_base():
    """Layout base oscuro para plotly."""
    return {
        'template': PLOTLY_TEMPLATE,
        'paper_bgcolor': 'rgba(0,0,0,0)',
        'plot_bgcolor': 'rgba(0,0,0,0)',
        'font': {'color': '#e8ecf4', 'family': 'Inter, sans-serif'},
        'margin': {'l': 20, 'r': 20, 't': 40, 'b': 40},
    }


# ─────────────────────────────────────────────────────────────────────────────
# Sidebar
# ─────────────────────────────────────────────────────────────────────────────
def sidebar_filtros(df_procesado):
    """Sidebar con carga de archivo y filtros globales. Retorna df filtrado."""
    st.sidebar.markdown("## 📦 TECU Despachos")
    st.sidebar.markdown("---")

    df_f = df_procesado.copy()
    total_rows = len(df_procesado)

    if df_procesado is None or total_rows == 0:
        return df_f

    st.sidebar.markdown("### 🔍 Filtros Globales")

    # ── MAPA DE MESES (Seguro y ordenado) ──
    df_meses = df_f[['Mes_Sort', 'Mes_Label']].dropna().drop_duplicates().sort_values('Mes_Sort')
    mapa_mes = dict(zip(df_meses['Mes_Sort'].astype(str), df_meses['Mes_Label'].astype(str)))
    
    # Opciones legibles para el multiselect
    opciones_mes = list(mapa_mes.values())
    sel_mes = st.sidebar.multiselect(
        "📅 Mes",
        options=['Todos'] + opciones_mes,
        default=['Todos'],
        key='ms_filtro_mes'
    )

    # ── TRANSPORTADORA ──
    opciones_transp = sorted(df_f['Transportadora'].dropna().unique().astype(str).tolist())
    sel_transp = st.sidebar.multiselect(
        "🚚 Transportadora",
        options=['Todas'] + opciones_transp,
        default=['Todas'],
        key='ms_filtro_transp'
    )

    # ── CIUDAD ──
    opciones_ciudad = sorted(df_f['Ciudad'].dropna().unique().astype(str).tolist())
    sel_ciudad = st.sidebar.multiselect(
        "📍 Ciudad",
        options=['Todas'] + opciones_ciudad,
        default=['Todas'],
        key='ms_filtro_ciudad'
    )

    # ── Aplicar filtros ──
    # Mes (comparación directa de labels para evitar fallos de mapeo)
    if 'Todos' not in sel_mes and len(sel_mes) > 0:
        df_f = df_f[df_f['Mes_Label'].astype(str).isin(sel_mes)]

    # Transportadora
    if 'Todas' not in sel_transp and len(sel_transp) > 0:
        df_f = df_f[df_f['Transportadora'].astype(str).isin(sel_transp)]

    # Ciudad
    if 'Todas' not in sel_ciudad and len(sel_ciudad) > 0:
        df_f = df_f[df_f['Ciudad'].astype(str).isin(sel_ciudad)]

    st.sidebar.markdown("---")
    curr_rows = len(df_f)
    st.sidebar.caption(f"📊 Registros: {curr_rows:,} / {total_rows:,}")
    
    if st.sidebar.button("🔄 Reiniciar App (Borrar Caché)"):
        st.cache_data.clear()
        st.rerun()

    return df_f


# ─────────────────────────────────────────────────────────────────────────────
# KPIs
# ─────────────────────────────────────────────────────────────────────────────
def _fila_kpis(indicadores, label_prefix=""):
    """Renderiza una fila de 5 métricas."""
    cols = st.columns(5)
    datos = [
        ("📦 Total Pedidos",       indicadores['total_pedidos'],
         None,                                               "Pedidos con status Entregado"),
        ("✅ % Cumplimiento NNS",  f"{indicadores['pct_cumplimiento']}%",
         f"{indicadores['cumplen_nns']} cumplen",           "Porcentaje dentro del SLA"),
        ("⚠️ Desvío Despacho",     indicadores['con_desvio_despacho'],
         f"Prom: {indicadores['promedio_desvio_despacho']}d", "Pedidos con retraso en despacho"),
        ("🔴 Desvío Entrega",      indicadores['con_desvio_entrega'],
         f"Prom: {indicadores['promedio_desvio_entrega']}d", "Pedidos fuera de SLA"),
        ("⏳ Pendientes (PTE)",     indicadores['pendientes'],
         None,                                               "Sin fecha de entrega registrada"),
    ]
    for col, (label, val, delta, help_txt) in zip(cols, datos):
        with col:
            st.metric(f"{label_prefix}{label}", val, delta, help=help_txt)


def mostrar_kpis(ind_global, ind_filtrado, etiqueta_filtro="Selección"):
    """Muestra KPIs en dos bloques: Global y Filtrado."""

    # ── Bloque GLOBAL ──────────────────────────────────
    st.markdown(
        "<p style='margin:0 0 6px 0; color:#8b9dc3; font-size:0.78rem; "
        "text-transform:uppercase; letter-spacing:0.06em;'>🌐 Total General (sin filtros)</p>",
        unsafe_allow_html=True
    )
    _fila_kpis(ind_global)

    st.markdown("<hr class='kpi-separator'>", unsafe_allow_html=True)

    # ── Bloque FILTRADO ────────────────────────────────
    # Calcular diferencia de cumplimiento para mostrarla
    delta_pct = round(ind_filtrado['pct_cumplimiento'] - ind_global['pct_cumplimiento'], 1)
    delta_str  = f"({'+' if delta_pct >= 0 else ''}{delta_pct}% vs global)"
    color_delta = "#22c55e" if delta_pct >= 0 else "#ef4444"

    st.markdown(
        f"<p style='margin:0 0 6px 0; color:#8b9dc3; font-size:0.78rem; "
        f"text-transform:uppercase; letter-spacing:0.06em;'>"
        f"🔍 {etiqueta_filtro} "
        f"<span style='color:{color_delta}; font-weight:700'>{delta_str}</span></p>",
        unsafe_allow_html=True
    )
    _fila_kpis(ind_filtrado)


# ─────────────────────────────────────────────────────────────────────────────
# Gráficos
# ─────────────────────────────────────────────────────────────────────────────
def mostrar_graficos(processor, df_filtrado):
    # ── Fila 1: Pie NNS + Bar desvíos ──
    col1, col2 = st.columns(2)

    with col1:
        st.markdown("### 🎯 Cumplimiento NNS")
        counts = df_filtrado['Cumple_NNS'].value_counts()
        fig = go.Figure(go.Pie(
            labels=counts.index.tolist(),
            values=counts.values.tolist(),
            hole=0.55,
            marker_colors=[
                COLOR_CUMPLE if l == 'Cumple'
                else COLOR_NO_CUMPLE if l == 'No cumple'
                else COLOR_PTE
                for l in counts.index
            ],
            textinfo='percent+label',
            textfont_size=13,
        ))
        fig.update_layout(**fig_base(), title_text='', showlegend=True)
        st.plotly_chart(fig, use_container_width=True)

    with col2:
        st.markdown("### 📊 Desvíos en Despacho vs Entrega")
        ind = processor.get_indicadores(df_filtrado)
        total_e = ind['total_pedidos'] if ind else 0
        categorias = ['Sin Desvío', 'Desvío Despacho', 'Desvío Entrega']
        valores = [
            max(0, total_e - (ind['con_desvio_despacho'] if ind else 0)),
            ind['con_desvio_despacho'] if ind else 0,
            ind['con_desvio_entrega'] if ind else 0,
        ]
        colores = ['#22c55e', '#f59e0b', '#ef4444']
        fig2 = go.Figure(go.Bar(
            x=categorias, y=valores,
            marker_color=colores,
            text=valores, textposition='outside',
        ))
        fig2.update_layout(**fig_base(), yaxis_title='Pedidos', showlegend=False)
        st.plotly_chart(fig2, use_container_width=True)

    # ── Fila 2: Ciudad ──
    st.markdown("### 📍 Cumplimiento por Ciudad (Top 12)")
    analisis_c = processor.get_analisis_ciudad(df_filtrado)
    if analisis_c is not None and len(analisis_c) > 0:
        top_c = analisis_c.head(12).copy()
        fig3 = go.Figure()
        fig3.add_trace(go.Bar(
            x=top_c['Ciudad'],
            y=top_c['Pct_Cumplimiento'],
            marker_color=[
                COLOR_CUMPLE if v >= 80 else COLOR_PTE if v >= 60 else COLOR_NO_CUMPLE
                for v in top_c['Pct_Cumplimiento']
            ],
            text=[f"{v}%" for v in top_c['Pct_Cumplimiento']],
            textposition='outside',
            customdata=top_c[['Total', 'No_Cumplen']].values,
            hovertemplate='<b>%{x}</b><br>Cumplimiento: %{y:.1f}%<br>'
                          'Total: %{customdata[0]}<br>No cumplen: %{customdata[1]}<extra></extra>'
        ))
        fig3.add_hline(y=80, line_dash='dash', line_color='#f59e0b',
                       annotation_text='Meta 80%', annotation_position='top left')
        fig3.update_layout(**fig_base(), yaxis_title='% Cumplimiento',
                           yaxis_range=[0, 115])
        st.plotly_chart(fig3, use_container_width=True)

    # ── Fila 3: Transportadora + Área ──
    col3, col4 = st.columns(2)

    with col3:
        st.markdown("### 🚚 Desempeño por Transportadora")
        analisis_t = processor.get_analisis_transportadora(df_filtrado)
        if analisis_t is not None and len(analisis_t) > 0:
            fig4 = px.bar(
                analisis_t.head(8),
                x='Transportadora', y='Pct_Cumplimiento',
                color='Desvio_Prom',
                color_continuous_scale=['#22c55e', '#f59e0b', '#ef4444'],
                text='Pct_Cumplimiento',
                custom_data=['Total', 'No_Cumplen', 'Desvio_Prom'],
                template=PLOTLY_TEMPLATE,
            )
            fig4.update_traces(
                texttemplate='%{text:.1f}%',
                textposition='outside',
                hovertemplate='<b>%{x}</b><br>Cumplimiento: %{y:.1f}%<br>'
                              'Total: %{customdata[0]}<br>No cumplen: %{customdata[1]}<br>'
                              'Desvío prom: %{customdata[2]:.1f}d<extra></extra>'
            )
            fig4.update_layout(**fig_base(), yaxis_title='% Cumplimiento',
                               yaxis_range=[0, 115], coloraxis_showscale=False)
            fig4.add_hline(y=80, line_dash='dash', line_color='#f59e0b')
            st.plotly_chart(fig4, use_container_width=True)

    with col4:
        st.markdown("### 🏢 Responsabilidad del Incumplimiento")
        inc = processor.get_pedidos_incumplimiento(df_filtrado)
        if inc is not None and len(inc) > 0 and 'Area_Incumple' in inc.columns:
            areas = inc['Area_Incumple'].value_counts()
            fig5 = go.Figure(go.Pie(
                labels=areas.index.tolist(),
                values=areas.values.tolist(),
                hole=0.45,
                textinfo='percent+label',
                textfont_size=11,
            ))
            fig5.update_layout(**fig_base())
            st.plotly_chart(fig5, use_container_width=True)
        else:
            st.success("🎉 Sin incumplimientos en el período seleccionado.")

    # ── Fila 4: Tendencia mensual ──
    st.markdown("### 📈 Evolución Mensual del Cumplimiento NNS")
    analisis_m = processor.get_analisis_mes(df_filtrado)
    if analisis_m is not None and len(analisis_m) > 0:
        fig6 = go.Figure()
        fig6.add_trace(go.Bar(
            x=analisis_m['Mes_Label'], y=analisis_m['Total'],
            name='Total pedidos',
            marker_color='#3b4a6b', opacity=0.7,
            yaxis='y2',
        ))
        fig6.add_trace(go.Scatter(
            x=analisis_m['Mes_Label'], y=analisis_m['Pct_Cumplimiento'],
            name='% Cumplimiento NNS',
            line=dict(color='#6366f1', width=3),
            mode='lines+markers+text',
            text=[f"{v}%" for v in analisis_m['Pct_Cumplimiento']],
            textposition='top center',
            textfont=dict(size=10, color='#a5b4fc'),
        ))
        fig6.add_hline(y=80, line_dash='dash', line_color='#f59e0b',
                       annotation_text='Meta 80%', annotation_position='bottom right')
        layout = fig_base()
        layout.update({
            'yaxis': {'title': '% Cumplimiento', 'range': [0, 115], 'side': 'left'},
            'yaxis2': {'title': 'Total Pedidos', 'overlaying': 'y', 'side': 'right', 'showgrid': False},
            'legend': {'orientation': 'h', 'y': -0.15},
        })
        fig6.update_layout(**layout)
        st.plotly_chart(fig6, use_container_width=True)
    else:
        st.info("Selecciona más de un mes para ver la tendencia.")


# ─────────────────────────────────────────────────────────────────────────────
# Recomendaciones
# ─────────────────────────────────────────────────────────────────────────────
def mostrar_recomendaciones(processor, df_filtrado):
    st.markdown("### 💡 Análisis de Mejora")
    recs = processor.get_recomendaciones(df_filtrado)

    if not recs:
        st.info("No hay suficientes datos para generar recomendaciones.")
        return

    css_class = {'success': 'rec-success', 'warning': 'rec-warning',
                 'error': 'rec-error', 'info': 'rec-info'}

    for titulo, cuerpo, tipo in recs:
        cls = css_class.get(tipo, '')
        st.markdown(
            f'<div class="rec-card {cls}">'
            f'<strong style="color:#e8ecf4">{titulo}</strong><br>'
            f'<span style="color:#8b9dc3;font-size:0.9rem">{cuerpo}</span>'
            f'</div>',
            unsafe_allow_html=True
        )


# ─────────────────────────────────────────────────────────────────────────────
# Tabla de detalle
# ─────────────────────────────────────────────────────────────────────────────
def mostrar_tabla_detalle(processor, df_filtrado):
    st.markdown("### 📋 Detalle de Incumplimientos")

    inc = processor.get_pedidos_incumplimiento(df_filtrado)

    if inc is None or len(inc) == 0:
        st.success("🎉 No hay pedidos con incumplimiento en el período seleccionado.")
        return

    # Sub-filtros
    cf1, cf2, cf3 = st.columns(3)
    with cf1:
        ciudades_inc = ['Todas'] + sorted(inc['Ciudad'].dropna().astype(str).unique().tolist())
        c_sel = st.selectbox("Ciudad", ciudades_inc, key='tab_ciudad')
    with cf2:
        areas_inc = ['Todas'] + sorted(inc['Area_Incumple'].dropna().unique().tolist())
        a_sel = st.selectbox("Área Responsable", areas_inc, key='tab_area')
    with cf3:
        min_d = float(inc['Desvio_Entrega'].min())
        max_d = float(inc['Desvio_Entrega'].max())
        d_sel = st.slider("Desvío mínimo (días)", min_value=min_d, max_value=max_d,
                          value=min_d, key='tab_desvio')

    df_t = inc.copy()
    if c_sel != 'Todas':
        df_t = df_t[df_t['Ciudad'].astype(str) == c_sel]
    if a_sel != 'Todas':
        df_t = df_t[df_t['Area_Incumple'] == a_sel]
    df_t = df_t[df_t['Desvio_Entrega'] >= d_sel]

    st.caption(f"Mostrando {len(df_t):,} de {len(inc):,} incumplimientos")

    # Renombrar para display
    rename_display = {
        'Fecha': 'Fecha Compra', 'No_Orden': 'No. Orden',
        'Cliente': 'Cliente', 'Producto': 'Producto',
        'Ciudad': 'Ciudad', 'Transportadora': 'Transportadora',
        'No_Guia': 'No. Guía', 'Fecha_Despacho': 'F. Despacho',
        'Fecha_Entrega': 'F. Entrega',
        'Dias_Despacho_Hab': 'Días Despacho', 'Dias_Entrega_Hab': 'Días Entrega',
        'SLA_Entrega': 'SLA', 'Desvio_Despacho': 'Desvío Despacho',
        'Desvio_Entrega': 'Desvío Entrega', 'Area_Incumple': 'Área Responsable',
    }
    df_show = df_t.rename(columns={k: v for k, v in rename_display.items() if k in df_t.columns})

    st.dataframe(df_show, use_container_width=True, hide_index=True)

    # Exportar
    col_exp1, col_exp2 = st.columns([1, 4])
    with col_exp1:
        xlsx_data = df_t.to_excel.__module__  # just a ref to confirm we can export
        try:
            import io
            buf = io.BytesIO()
            df_t.to_excel(buf, index=False, sheet_name='Incumplimientos')
            buf.seek(0)
            st.download_button(
                "📥 Exportar a Excel",
                data=buf,
                file_name="incumplimientos_filtrados.xlsx",
                mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
            )
        except Exception:
            pass


# ─────────────────────────────────────────────────────────────────────────────
# MAIN
# ─────────────────────────────────────────────────────────────────────────────
def main():
    # ── Sidebar – carga de archivo ──
    st.sidebar.markdown("### 📂 Cargar Archivo")
    uploaded = st.sidebar.file_uploader(
        "Archivo Excel (.xlsx / .xls)",
        type=['xlsx', 'xls'],
        help="Sube el archivo de Seguimiento de Despachos TECU"
    )

    if uploaded is None:
        # Pantalla de bienvenida
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
                    "- Área responsable")
        with col_i3:
            st.info("**🔍 Filtros disponibles**\n\n"
                    "- Por Mes\n"
                    "- Por Transportadora\n"
                    "- Por Ciudad")

        st.markdown("\n#### 👆 Sube el archivo Excel en el panel izquierdo para comenzar.")
        return

    # ── Procesar datos ──
    with st.spinner("⏳ Procesando datos..."):
        processor, df_procesado, hoja = cargar_y_procesar(uploaded)

    if processor is None or df_procesado is None:
        st.error("No se pudo procesar el archivo. Verifica el formato.")
        return

    # ── Sidebar filtros → df filtrado ──
    df_filtrado = sidebar_filtros(df_procesado)

    # ── Exportar completo ──
    st.sidebar.markdown("---")
    st.sidebar.markdown("### 📥 Exportar")
    try:
        import io
        buf = io.BytesIO()
        df_filtrado.to_excel(buf, index=False, sheet_name='Base Analizada')
        buf.seek(0)
        st.sidebar.download_button(
            "⬇️ Descargar Base Analizada",
            data=buf,
            file_name="Base_Ventas_Analizada.xlsx",
            mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
        )
    except Exception:
        pass

    # ── Header ──
    st.markdown("# 📊 Dashboard de Despachos TECU Aura `v1.3` 🚀")
    if len(df_filtrado) < len(df_procesado):
        st.info(f"💡 Filtro Activo: Viendo {len(df_filtrado)} de {len(df_procesado)} registros.")
    st.caption(
        f"**Archivo:** `{uploaded.name}` &nbsp;|&nbsp; "
        f"**Hoja:** `{hoja}` &nbsp;|&nbsp; "
        f"**Registros seleccionados:** {len(df_filtrado):,} / {len(df_procesado):,}"
    )
    st.markdown("---")

    # ── Indicadores globales (siempre el total del archivo) ──
    ind_global = processor.get_indicadores(df_procesado)

    # ── Indicadores filtrados ──
    indicadores = processor.get_indicadores(df_filtrado)

    if indicadores is None or indicadores['total_pedidos'] == 0:
        st.warning("⚠️ No hay pedidos con status 'Entregado' en el rango seleccionado.")
        st.dataframe(df_filtrado.head(20), use_container_width=True)
        return

    # Etiqueta dinámica para el bloque filtrado
    es_global = len(df_filtrado) == len(df_procesado)
    etiqueta = "Total General con filtros " if es_global else "Selección Actual"

    mostrar_kpis(ind_global, indicadores, etiqueta)
    st.markdown("---")

    # ── Gráficos ──
    mostrar_graficos(processor, df_filtrado)
    st.markdown("---")

    # ── Recomendaciones ──
    mostrar_recomendaciones(processor, df_filtrado)
    st.markdown("---")

    # ── Tabla detalle ──
    mostrar_tabla_detalle(processor, df_filtrado)


if __name__ == "__main__":
    main()