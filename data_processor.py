"""
Procesador de datos de despachos TECU
"""

import pandas as pd
import numpy as np
from utils import (
    calcular_dias_habiles,
    determinar_sla_entrega,
    determinar_area_incumple,
    evaluar_cumple_nns
)

# ─────────────────────────────────────────────
# Mapa flexible de columnas del Excel
# Clave: nombre canónico interno | Valor: posibles nombres en el Excel
# ─────────────────────────────────────────────
COLUMN_MAP = {
    'Fecha':                 ['fecha', 'fecha compra', 'fecha de compra', 'fecha pedido', 'fecha de pedido'],
    'Cliente':               ['cliente', 'cliente/proveedor', 'cliente / proveedor', 'nombre cliente'],
    'Producto':              ['producto', 'referencia', 'codigo producto', 'código producto'],
    'Descripcion':           ['descripcion del producto', 'descripción del producto', 'descripcion', 'descripción', 'detalle'],
    'Ciudad':                ['ciudad', 'ciudad entrega', 'ciudad de entrega', 'destino', 'lugar entrega', 'lugar de entrega'],
    'Status':                ['status', 'status entrega', 'estado', 'estado entrega', 'estatus'],
    'Transportadora':        ['transportadora', 'transporte', 'operador logístico', 'operador logistico'],
    'No_Guia':               ['no guia', 'número de guía', 'numero de guia', 'guia', 'guía', 'no. guia'],
    'Fecha_Despacho':        ['fecha de despacho', 'fecha despacho', 'despacho'],
    'Fecha_Entrega':         ['fecha de entrega', 'fecha entrega', 'entrega'],
    'No_Orden':              ['no orden', 'no. orden', 'orden', 'numero orden', 'número orden', 'no_orden'],
}

MESES_ES = {
    1: 'Ene', 2: 'Feb', 3: 'Mar', 4: 'Abr',
    5: 'May', 6: 'Jun', 7: 'Jul', 8: 'Ago',
    9: 'Sep', 10: 'Oct', 11: 'Nov', 12: 'Dic'
}


class DataProcessor:
    def __init__(self, df):
        self.df_original = df.copy()
        self.df_procesado = None

    # ─────────────────────────────────────────
    # Resolución robusta de columnas
    # ─────────────────────────────────────────
    def _resolver_columnas(self, df):
        """Mapea columnas del Excel a nombres canónicos."""
        col_lower = {str(c).strip().lower(): c for c in df.columns}
        rename = {}
        for canonical, variants in COLUMN_MAP.items():
            for v in variants:
                if v in col_lower:
                    rename[col_lower[v]] = canonical
                    break

        df = df.rename(columns=rename)

        # Agregar columnas faltantes como None
        for canonical in COLUMN_MAP:
            if canonical not in df.columns:
                df[canonical] = None

        return df

    # ─────────────────────────────────────────
    # Pipeline principal
    # ─────────────────────────────────────────
    def procesar(self, sla_almacen=1, sla_principal=3, sla_otras=5):
        """Ejecuta todo el pipeline de procesamiento con parámetros de SLA."""
        df = self.df_original.copy()
        df.columns = df.columns.str.strip()

        # Eliminar filas completamente vacías
        df = df.dropna(how='all')

        # Mapear columnas
        df = self._resolver_columnas(df)

        # Asegurar tipos de fecha
        for col_fecha in ['Fecha', 'Fecha_Despacho', 'Fecha_Entrega']:
            if col_fecha in df.columns:
                df[col_fecha] = pd.to_datetime(df[col_fecha], errors='coerce')

        # ── Días hábiles ──────────────────────
        df['Dias_Despacho_Hab'] = df.apply(
            lambda r: calcular_dias_habiles(r['Fecha'], r['Fecha_Despacho']), axis=1
        )
        df['Dias_Entrega_Hab'] = df.apply(
            lambda r: calcular_dias_habiles(r['Fecha_Despacho'], r['Fecha_Entrega']), axis=1
        )

        # ── SLA y desvíos ─────────────────────
        # Nota: El desvío de entrega se calcula DESDE EL DESPACHO para evaluar transportadora
        df['SLA_Entrega'] = df['Ciudad'].apply(lambda c: determinar_sla_entrega(c, sla_principal, sla_otras))
        
        df['Desvio_Despacho'] = df.apply(
            lambda r: (r['Dias_Despacho_Hab'] - sla_almacen)
            if pd.notna(r['Dias_Despacho_Hab']) else None, axis=1
        )
        df['Desvio_Entrega'] = df.apply(
            lambda r: (r['Dias_Entrega_Hab'] - r['SLA_Entrega'])
            if pd.notna(r['Dias_Entrega_Hab']) else None, axis=1
        )

        # ── Cumplimiento NNS ──────────────────
        # NNS se basa en el cumplimiento global (Compra -> Entrega)
        # Recalculamos días totales compra->entrega para el indicador NNS
        def _get_nns(r):
            dias_totales = calcular_dias_habiles(r['Fecha'], r['Fecha_Entrega'])
            if pd.isna(dias_totales): return 'PTE'
            # El SLA total es SLA_Almacén + SLA_Ciudad
            sla_total = sla_almacen + r['SLA_Entrega']
            return 'Cumple' if dias_totales <= sla_total else 'No cumple'

        df['Cumple_NNS'] = df.apply(_get_nns, axis=1)

        # ── Área responsable ──────────────────
        df['Area_Incumple'] = df.apply(
            lambda r: determinar_area_incumple(
                r['Desvio_Despacho'], r['Desvio_Entrega'], r['Transportadora']
            ), axis=1
        )

        # ── Mes para filtros ──────────────────
        df['Mes_Sort'] = df['Fecha'].dt.strftime('%Y-%m')
        
        def _fmt_mes(x):
            if pd.isna(x): return None
            # MESES_ES importado de arriba
            m = MESES_ES.get(x.month, str(x.month))
            y = str(x.year)[2:]
            return f"{m}-{y}"
            
        df['Mes_Label'] = df['Fecha'].apply(_fmt_mes)

        self.df_procesado = df
        return df

    # ─────────────────────────────────────────
    # Helper: filtrar entregados de forma segura
    # ─────────────────────────────────────────
    def _filtrar_entregados(self, df):
        """Retorna SOLO filas con Status == 'Entregado'.
        Si no hay ninguna, retorna un DataFrame vacío (NO el original)."""
        if 'Status' not in df.columns:
            return df
        mask = df['Status'].astype(str).str.strip().str.lower() == 'entregado'
        return df[mask]

    # ─────────────────────────────────────────
    # Indicadores (recibe df ya filtrado)
    # ─────────────────────────────────────────
    def get_indicadores(self, df=None):
        if df is None:
            df = self.df_procesado
        if df is None or len(df) == 0:
            return None

        df_ent = self._filtrar_entregados(df)
        total = len(df_ent)
        if total == 0:
            return None

        cumplen = (df_ent['Cumple_NNS'] == 'Cumple').sum()
        no_cumplen = (df_ent['Cumple_NNS'] == 'No cumple').sum()
        pendientes = (df_ent['Cumple_NNS'] == 'PTE').sum()

        des_desp = df_ent[df_ent['Desvio_Despacho'] > 0]
        des_entr = df_ent[df_ent['Desvio_Entrega'] > 0]

        return {
            'total_pedidos': int(total),
            'cumplen_nns': int(cumplen),
            'no_cumplen_nns': int(no_cumplen),
            'pendientes': int(pendientes),
            'pct_cumplimiento': float(round((cumplen / total * 100), 1)) if total > 0 else 0.0,
            'con_desvio_despacho': int(len(des_desp)),
            'con_desvio_entrega': int(len(des_entr)),
            'promedio_desvio_despacho': float(round(des_desp['Desvio_Despacho'].mean(), 1)) if len(des_desp) > 0 else 0.0,
            'promedio_desvio_entrega': float(round(des_entr['Desvio_Entrega'].mean(), 1)) if len(des_entr) > 0 else 0.0,
            'max_desvio_despacho': int(des_desp['Desvio_Despacho'].max()) if len(des_desp) > 0 else 0,
            'max_desvio_entrega': int(des_entr['Desvio_Entrega'].max()) if len(des_entr) > 0 else 0,
        }

    # ─────────────────────────────────────────
    # Análisis por ciudad
    # ─────────────────────────────────────────
    def get_analisis_ciudad(self, df=None):
        if df is None:
            df = self.df_procesado
        if df is None:
            return None

        df_e = self._filtrar_entregados(df)

        agg = df_e.groupby('Ciudad').agg(
            Total=('Cumple_NNS', 'count'),
            Cumplen=('Cumple_NNS', lambda x: (x == 'Cumple').sum()),
            Desvio_Prom=('Desvio_Entrega', 'mean')
        ).reset_index()
        agg['No_Cumplen'] = agg['Total'] - agg['Cumplen']
        agg['Pct_Cumplimiento'] = (agg['Cumplen'] / agg['Total'] * 100).round(1)
        agg['Desvio_Prom'] = agg['Desvio_Prom'].round(2)
        return agg.sort_values('Total', ascending=False)

    # ─────────────────────────────────────────
    # Análisis por transportadora
    # ─────────────────────────────────────────
    def get_analisis_transportadora(self, df=None):
        if df is None:
            df = self.df_procesado
        if df is None:
            return None

        df_e = self._filtrar_entregados(df)

        agg = df_e.groupby('Transportadora').agg(
            Total=('Cumple_NNS', 'count'),
            Cumplen=('Cumple_NNS', lambda x: (x == 'Cumple').sum()),
            Desvio_Prom=('Desvio_Entrega', 'mean')
        ).reset_index()
        agg['No_Cumplen'] = agg['Total'] - agg['Cumplen']
        agg['Pct_Cumplimiento'] = (agg['Cumplen'] / agg['Total'] * 100).round(1)
        agg['Desvio_Prom'] = agg['Desvio_Prom'].round(2)
        return agg.sort_values('Total', ascending=False)

    # ─────────────────────────────────────────
    # Tendencia mensual
    # ─────────────────────────────────────────
    def get_analisis_mes(self, df=None):
        if df is None:
            df = self.df_procesado
        if df is None or 'Mes_Sort' not in df.columns:
            return None

        df_e = self._filtrar_entregados(df)

        agg = df_e.groupby(['Mes_Sort', 'Mes_Label']).agg(
            Total=('Cumple_NNS', 'count'),
            Cumplen=('Cumple_NNS', lambda x: (x == 'Cumple').sum()),
            Desvio_Prom_Entrega=('Desvio_Entrega', 'mean'),
            Desvio_Prom_Despacho=('Desvio_Despacho', 'mean'),
        ).reset_index()
        agg['Pct_Cumplimiento'] = (agg['Cumplen'] / agg['Total'] * 100).round(1)
        agg = agg.sort_values('Mes_Sort')
        return agg

    # ─────────────────────────────────────────
    # Pedidos con incumplimiento
    # ─────────────────────────────────────────
    def get_pedidos_incumplimiento(self, df=None):
        if df is None:
            df = self.df_procesado
        if df is None:
            return None

        df_e = self._filtrar_entregados(df)
        inc = df_e[df_e['Cumple_NNS'] == 'No cumple'].copy()

        cols = [c for c in [
            'Fecha', 'No_Orden', 'Cliente', 'Producto', 'Descripcion',
            'Ciudad', 'Transportadora', 'No_Guia',
            'Fecha_Despacho', 'Fecha_Entrega',
            'Dias_Despacho_Hab', 'Dias_Entrega_Hab',
            'SLA_Entrega', 'Desvio_Despacho', 'Desvio_Entrega',
            'Cumple_NNS', 'Area_Incumple', 'Mes_Label'
        ] if c in inc.columns]

        return inc[cols]

    # ─────────────────────────────────────────
    # Recomendaciones de mejora
    # ─────────────────────────────────────────
    def get_recomendaciones(self, df=None):
        if df is None:
            df = self.df_procesado
        if df is None:
            return []

        df_e = self._filtrar_entregados(df)
        total = len(df_e)
        if total == 0:
            return []

        recs = []
        cumplen = (df_e['Cumple_NNS'] == 'Cumple').sum()
        pct = round(cumplen / total * 100, 1) if total > 0 else 0

        # ── Cumplimiento global ──
        if pct >= 90:
            recs.append(('✅ Excelente desempeño',
                         f'El {pct}% de cumplimiento NNS supera el objetivo de 80%. Mantener el nivel actual.',
                         'success'))
        elif pct >= 80:
            recs.append(('✅ Cumplimiento dentro del objetivo',
                         f'{pct}% de cumplimiento NNS. Hay margen de mejora para alcanzar 90%+.',
                         'info'))
        else:
            recs.append(('⚠️ Cumplimiento por debajo del objetivo',
                         f'Solo el {pct}% cumple NNS. Meta mínima: 80%. Se requiere acción inmediata.',
                         'warning'))

        # ── Peor transportadora ──
        try:
            analisis_t = self.get_analisis_transportadora(df)
            if analisis_t is not None and len(analisis_t) > 0:
                peor_t = analisis_t.sort_values('Pct_Cumplimiento').iloc[0]
                if peor_t['Pct_Cumplimiento'] < 70 and peor_t['Total'] >= 3:
                    recs.append((
                        f'🚚 Transportadora crítica: {peor_t["Transportadora"]}',
                        f'{peor_t["Transportadora"]} tiene solo {peor_t["Pct_Cumplimiento"]}% de cumplimiento '
                        f'({int(peor_t["No_Cumplen"])} incumplimientos de {int(peor_t["Total"])} pedidos). '
                        f'Desvío promedio: {peor_t["Desvio_Prom"]:.1f} días. '
                        'Revisar contrato y evaluar alternativas.',
                        'error'
                    ))
        except Exception:
            pass

        # ── Peor ciudad ──
        try:
            analisis_c = self.get_analisis_ciudad(df)
            if analisis_c is not None and len(analisis_c) > 0:
                peor_c = analisis_c.sort_values('Pct_Cumplimiento').iloc[0]
                if peor_c['Pct_Cumplimiento'] < 70 and peor_c['Total'] >= 3:
                    sla_ciud = 3 if peor_c["Pct_Cumplimiento"] < 80 else 5
                    recs.append((
                        f'📍 Ciudad con más fallos: {peor_c["Ciudad"]}',
                        f'{peor_c["Ciudad"]} tiene {peor_c["Pct_Cumplimiento"]}% de cumplimiento '
                        f'({int(peor_c["No_Cumplen"])} de {int(peor_c["Total"])} pedidos fuera de SLA). '
                        'Verificar cobertura de la transportadora y rutas.',
                        'warning'
                    ))
        except Exception:
            pass

        # ── Desvío de despacho ──
        try:
            des_desp = df_e[df_e['Desvio_Despacho'] > 0]
            pct_desp = round(len(des_desp) / total * 100, 1)
            if pct_desp > 20:
                prom_desp = des_desp['Desvio_Despacho'].mean()
                recs.append((
                    '📦 Alto desvío en despacho (Almacén)',
                    f'{pct_desp}% de los pedidos tarda más de 1 día hábil en ser despachado. '
                    f'Promedio de retraso: {prom_desp:.1f} días. '
                    'Revisar proceso de picking, alistamiento y horarios de corte.',
                    'warning'
                ))
        except Exception:
            pass

        # ── Tendencia mes a mes ──
        try:
            analisis_m = self.get_analisis_mes(df)
            if analisis_m is not None and len(analisis_m) >= 2:
                ult_mes = analisis_m.iloc[-1]
                pen_mes = analisis_m.iloc[-2]
                delta = round(ult_mes['Pct_Cumplimiento'] - pen_mes['Pct_Cumplimiento'], 1)
                if delta < -10:
                    recs.append((
                        f'📉 Caída en cumplimiento: {ult_mes["Mes_Label"]}',
                        f'El mes {ult_mes["Mes_Label"]} bajó {abs(delta)} puntos porcentuales respecto a {pen_mes["Mes_Label"]}. '
                        'Analizar causas: temporada alta, cambio de transportadora, o problemas de inventario.',
                        'error'
                    ))
                elif delta > 10:
                    recs.append((
                        f'📈 Mejora notable en {ult_mes["Mes_Label"]}',
                        f'El mes {ult_mes["Mes_Label"]} mejoró {delta} puntos respecto a {pen_mes["Mes_Label"]}. '
                        'Identificar qué prácticas mejoraron el desempeño y replicarlas.',
                        'success'
                    ))
        except Exception:
            pass

        return recs

    # ─────────────────────────────────────────
    # MEGA REPORTE (Exportación v1.7)
    # ─────────────────────────────────────────
    def generate_mega_report(self, df_filtrado, ind_filtrado, ind_global):
        """
        Genera un objeto BytesIO con un Excel de múltiples pestañas.
        """
        import io
        buf = io.BytesIO()
        
        with pd.ExcelWriter(buf, engine='xlsxwriter') as writer:
            # 1. Resumen Ejecutivo (KPIs)
            resumen_data = {
                'Métrica': [
                    'Total Pedidos', '% Cumplimiento NNS', 'Pedidos Cumplen', 
                    'Pedidos No Cumplen', 'Pendientes (PTE)',
                    'Pedidos con Desvío Despacho', 'Promedio Desvío Despacho (días)',
                    'Pedidos con Desvío Entrega', 'Promedio Desvío Entrega (días)'
                ],
                'Selección Actual': [
                    ind_filtrado['total_pedidos'], f"{ind_filtrado['pct_cumplimiento']}%",
                    ind_filtrado['cumplen_nns'], ind_filtrado['no_cumplen_nns'],
                    ind_filtrado['pendientes'], ind_filtrado['con_desvio_despacho'],
                    ind_filtrado['promedio_desvio_despacho'], ind_filtrado['con_desvio_entrega'],
                    ind_filtrado['promedio_desvio_entrega']
                ],
                'Total General': [
                    ind_global['total_pedidos'], f"{ind_global['pct_cumplimiento']}%",
                    ind_global['cumplen_nns'], ind_global['no_cumplen_nns'],
                    ind_global['pendientes'], ind_global['con_desvio_despacho'],
                    ind_global['promedio_desvio_despacho'], ind_global['con_desvio_entrega'],
                    ind_global['promedio_desvio_entrega']
                ]
            }
            pd.DataFrame(resumen_data).to_excel(writer, sheet_name='Resumen Ejecutivo', index=False)
            
            # 2. Recomendaciones
            recs = self.get_recomendaciones(df_filtrado)
            if recs:
                recs_df = pd.DataFrame(recs, columns=['Título', 'Detalle', 'Nivel'])
                recs_df.to_excel(writer, sheet_name='Hallazgos y Recomendaciones', index=False)
            
            # 3. Análisis por Ciudad
            df_ciud = self.get_analisis_ciudad(df_filtrado)
            if df_ciud is not None:
                df_ciud.to_excel(writer, sheet_name='Análisis por Ciudad', index=False)
                
            # 4. Análisis por Transportadora
            df_trans = self.get_analisis_transportadora(df_filtrado)
            if df_trans is not None:
                df_trans.to_excel(writer, sheet_name='Análisis por Transportadora', index=False)
                
            # 5. Base Completa (Filtrada)
            df_filtrado.to_excel(writer, sheet_name='Base de Datos (Selección)', index=False)
            
            # Ajustar anchos de columna (opcional pero profesional)
            workbook = writer.book
            header_format = workbook.add_format({'bold': True, 'bg_color': '#D7E4BC', 'border': 1})
            
            for sheet in writer.sheets.values():
                sheet.set_column('A:Z', 20)

        buf.seek(0)
        return buf

    def exportar_excel(self, path, df=None):
        if df is None:
            df = self.df_procesado
        if df is not None:
            df.to_excel(path, index=False, sheet_name='Base Analizada')
            return True
        return False