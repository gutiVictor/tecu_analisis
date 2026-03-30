"""
MÓDULO DE PROCESAMIENTO DE DATOS - TECU Aura
"""

import pandas as pd
import numpy as np
from datetime import datetime
import io


class DataProcessor:
    """Clase principal para procesar datos de despachos TECU."""
    
    def __init__(self, df: pd.DataFrame):
        """Inicializa el procesador con el DataFrame crudo."""
        self.df_original = df.copy()
        self.df_procesado = None
    
    def procesar(self, sla_almacen: int = 1, sla_principal: int = 3, sla_otras: int = 5) -> pd.DataFrame:
        """
        Procesa el DataFrame aplicando transformaciones y cálculos de SLA.
        """
        df = self.df_original.copy()
        
        # ── LIMPIEZA BÁSICA ──────────────────────────────────────────────
        df = df.dropna(how='all')  # Eliminar filas completamente vacías
        
        # Eliminar filas sin número de orden válido
        if 'No orden' in df.columns:
            df = df[df['No orden'].notna()]
            df['No orden'] = df['No orden'].astype(str).str.strip()
            df = df[(df['No orden'] != '') & (df['No orden'] != 'nan')]
        
        # ── MAPEO DE COLUMNAS ──────────────────────────────────────────────
        column_mapping = {
            'No orden': 'No_Orden',
            'Fecha Venta': 'Fecha',
            'Cliente/Proveedor': 'Cliente',
            'Codigo': 'Producto',
            'Categoria': 'Categoria',
            'Ciudad': 'Ciudad',
            'Transportadora': 'Transportadora',
            'No guia': 'No_Guia',
            'Fecha de despacho': 'Fecha_Despacho',
            'Fecha de Entrega': 'Fecha_Entrega',
            'Status entrega': 'Status_Entrega',
            'Status Despacho': 'Status_Despacho',
            'Cumple NNS': 'Cumple_NNS',
            'Reponsable Incumplimiento': 'Area_Incumple',
            'Responsable Incumplimiento': 'Area_Incumple',
            'Valor despacho': 'Valor_despacho',
            'Causal de Incumplimiento': 'Causal_Incumplimiento',
            'Observaciones': 'Observaciones',
            'Concepto': 'Concepto',
            'Mes': 'Mes_Label',
        }
        
        df = df.rename(columns={k: v for k, v in column_mapping.items() if k in df.columns})
        
        # ── CREAR Mes_Sort DESDE Mes_Label (CRÍTICO) ──────────────────────────────────────────────
        mes_a_numero = {
            'enero': 1, 'febrero': 2, 'marzo': 3, 'abril': 4,
            'mayo': 5, 'junio': 6, 'julio': 7, 'agosto': 8,
            'septiembre': 9, 'octubre': 10, 'noviembre': 11, 'diciembre': 12,
            'Enero': 1, 'Febrero': 2, 'Marzo': 3, 'Abril': 4,
            'Mayo': 5, 'Junio': 6, 'Julio': 7, 'Agosto': 8,
            'Septiembre': 9, 'Octubre': 10, 'Noviembre': 11, 'Diciembre': 12,
            'ENERO': 1, 'FEBRERO': 2, 'MARZO': 3, 'ABRIL': 4,
            'MAYO': 5, 'JUNIO': 6, 'JULIO': 7, 'AGOSTO': 8,
            'SEPTIEMBRE': 9, 'OCTUBRE': 10, 'NOVIEMBRE': 11, 'DICIEMBRE': 12,
        }
        
        # Crear Mes_Sort desde Mes_Label
        if 'Mes_Label' in df.columns:
            df['Mes_Sort'] = df['Mes_Label'].map(mes_a_numero)
            # Si hay valores NaN, intentar desde Fecha
            if df['Mes_Sort'].isna().any() and 'Fecha' in df.columns:
                df['Fecha'] = pd.to_datetime(df['Fecha'], errors='coerce', dayfirst=True)
                df['Mes_Sort'] = df['Mes_Sort'].fillna(df['Fecha'].dt.month)
                df['Mes_Label'] = df['Mes_Label'].fillna(df['Fecha'].dt.month_name(locale='es_ES'))
        elif 'Fecha' in df.columns:
            df['Fecha'] = pd.to_datetime(df['Fecha'], errors='coerce', dayfirst=True)
            df['Mes_Sort'] = df['Fecha'].dt.month
            df['Mes_Label'] = df['Fecha'].dt.month_name(locale='es_ES')
        else:
            # Fallback: valores por defecto
            df['Mes_Sort'] = 1
            df['Mes_Label'] = 'Enero'
        
        # ── VALIDAR QUE Mes_Sort EXISTE ──────────────────────────────────────────────
        if 'Mes_Sort' not in df.columns:
            df['Mes_Sort'] = 1
        if 'Mes_Label' not in df.columns:
            df['Mes_Label'] = 'Enero'
        
        # ── PROCESAR FECHAS ──────────────────────────────────────────────
        fecha_cols = ['Fecha', 'Fecha_Despacho', 'Fecha_Entrega']
        for col in fecha_cols:
            if col in df.columns:
                df[col] = pd.to_datetime(df[col], errors='coerce', dayfirst=True)
        
        # ── NORMALIZAR VALORES MONETARIOS ──────────────────────────────────────────────
        if 'Valor_despacho' in df.columns:
            df['Valor_num'] = df['Valor_despacho'].astype(str).str.replace(
                r'[^\d.]', '', regex=True
            ).replace('', '0').astype(float)
        
        # ── CALCULAR DÍAS DE ENTREGA Y DESPACHO ──────────────────────────────────────────────
        if 'Fecha' in df.columns and 'Fecha_Entrega' in df.columns:
            df['Dias_Entrega_Hab'] = (df['Fecha_Entrega'] - df['Fecha']).dt.days
            df['Dias_Entrega_Hab'] = df['Dias_Entrega_Hab'].clip(lower=0).fillna(0)
        
        if 'Fecha' in df.columns and 'Fecha_Despacho' in df.columns:
            df['Dias_Despacho_Hab'] = (df['Fecha_Despacho'] - df['Fecha']).dt.days
            df['Dias_Despacho_Hab'] = df['Dias_Despacho_Hab'].clip(lower=0).fillna(0)
        
        # ── CALCULAR DESVÍOS ──────────────────────────────────────────────
        df['Desvio_Entrega'] = 0.0
        df['Desvio_Despacho'] = 0.0
        
        ciudades_principales = ['Bogotá', 'Medellín', 'Cali', 'Bogotá y alrededores']
        
        if 'Dias_Entrega_Hab' in df.columns:
            for idx, row in df.iterrows():
                if pd.isna(row.get('Fecha_Entrega')):
                    continue
                ciudad = str(row.get('Ciudad', '')).lower()
                dias = row.get('Dias_Entrega_Hab', 0)
                
                if any(cp.lower() in ciudad for cp in ciudades_principales):
                    sla = sla_principal
                else:
                    sla = sla_otras
                
                if dias > sla:
                    df.at[idx, 'Desvio_Entrega'] = dias - sla
        
        if 'Dias_Despacho_Hab' in df.columns:
            df['Desvio_Despacho'] = df['Dias_Despacho_Hab'].clip(lower=0)
            df.loc[df['Desvio_Despacho'] <= sla_almacen, 'Desvio_Despacho'] = 0
        
        # ── NORMALIZAR CUMPLIMIENTO NNS ──────────────────────────────────────────────
        if 'Cumple_NNS' in df.columns:
            df['Cumple_NNS'] = df['Cumple_NNS'].astype(str).str.strip()
            
            df['Cumple_NNS'] = df['Cumple_NNS'].replace({
                'CUMPLE': 'Cumple', 'cumple': 'Cumple',
                'NO CUMPLE': 'No cumple', 'no cumple': 'No cumple',
                'PTE': 'PTE', 'pte': 'PTE',
                '#N/D': 'PTE', 'NAN': 'PTE', 'nan': 'PTE',
                'FALSO': 'PTE', 'Falso': 'PTE', 'falso': 'PTE',
                '0': 'PTE',
            })
            
            mask_cumple = (df['Cumple_NNS'].isin(['', 'nan', 'NAN'])) & (df['Fecha_Entrega'].notna())
            df.loc[mask_cumple, 'Cumple_NNS'] = 'Cumple'
            
            mask_pte = df['Fecha_Entrega'].isna()
            df.loc[mask_pte, 'Cumple_NNS'] = 'PTE'
        else:
            df['Cumple_NNS'] = 'PTE'
        
        # ── NORMALIZAR ÁREA DE INCUMPLIMIENTO ──────────────────────────────────────────────
        if 'Area_Incumple' in df.columns:
            df['Area_Incumple'] = df['Area_Incumple'].astype(str).str.strip()
            df.loc[df['Cumple_NNS'] == 'Cumple', 'Area_Incumple'] = ''
        else:
            df['Area_Incumple'] = ''
        
        # ── NORMALIZAR CAUSAL DE INCUMPLIMIENTO ──────────────────────────────────────────────
        if 'Causal_Incumplimiento' not in df.columns:
            df['Causal_Incumplimiento'] = ''
        
        # ── GUARDAR DATAFRAME PROCESADO ──────────────────────────────────────────────
        self.df_procesado = df
        return df
    
    def get_indicadores(self, df: pd.DataFrame) -> dict:
        """Calcula los KPIs principales del dashboard."""
        if df is None or len(df) == 0:
            return {
                'total_pedidos': 0, 'pct_cumplimiento': 0.0, 'cumplen_nns': 0,
                'con_desvio_despacho': 0, 'promedio_desvio_despacho': 0.0,
                'con_desvio_entrega': 0, 'promedio_desvio_entrega': 0.0, 'pendientes': 0,
            }
        
        total_pedidos = len(df)
        
        if 'Cumple_NNS' in df.columns:
            cumplen = len(df[df['Cumple_NNS'] == 'Cumple'])
            no_cumplen = len(df[df['Cumple_NNS'] == 'No cumple'])
            pendientes = len(df[df['Cumple_NNS'] == 'PTE'])
            pct_cumplimiento = round((cumplen / len(df) * 100), 1) if len(df) > 0 else 0.0
        else:
            cumplen = no_cumplen = pendientes = 0
            pct_cumplimiento = 0.0
        
        if 'Desvio_Despacho' in df.columns:
            con_desvio_despacho = len(df[df['Desvio_Despacho'] > 0])
            promedio_desvio_despacho = round(
                df.loc[df['Desvio_Despacho'] > 0, 'Desvio_Despacho'].mean(), 1
            ) if con_desvio_despacho > 0 else 0.0
        else:
            con_desvio_despacho = 0
            promedio_desvio_despacho = 0.0
        
        if 'Desvio_Entrega' in df.columns:
            con_desvio_entrega = len(df[df['Desvio_Entrega'] > 0])
            promedio_desvio_entrega = round(
                df.loc[df['Desvio_Entrega'] > 0, 'Desvio_Entrega'].mean(), 1
            ) if con_desvio_entrega > 0 else 0.0
        else:
            con_desvio_entrega = 0
            promedio_desvio_entrega = 0.0
        
        return {
            'total_pedidos': total_pedidos,
            'pct_cumplimiento': pct_cumplimiento,
            'cumplen_nns': cumplen,
            'no_cumplen_nns': no_cumplen,
            'con_desvio_despacho': con_desvio_despacho,
            'promedio_desvio_despacho': promedio_desvio_despacho,
            'con_desvio_entrega': con_desvio_entrega,
            'promedio_desvio_entrega': promedio_desvio_entrega,
            'pendientes': pendientes,
        }
    
    def get_analisis_ciudad(self, df: pd.DataFrame) -> pd.DataFrame:
        """Genera análisis de cumplimiento agrupado por ciudad."""
        if 'Ciudad' not in df.columns or len(df) == 0:
            return pd.DataFrame()
        
        analisis = df.groupby('Ciudad').agg(
            Total=('No_Orden', 'count'),
            Cumplen=('Cumple_NNS', lambda x: (x == 'Cumple').sum()),
            No_Cumplen=('Cumple_NNS', lambda x: (x == 'No cumple').sum())
        ).reset_index()
        
        analisis['Pct_Cumplimiento'] = round(analisis['Cumplen'] / analisis['Total'] * 100, 1)
        analisis = analisis.sort_values('Total', ascending=False)
        
        return analisis
    
    def get_analisis_transportadora(self, df: pd.DataFrame) -> pd.DataFrame:
        """Genera análisis de desempeño por transportadora."""
        if 'Transportadora' not in df.columns or len(df) == 0:
            return pd.DataFrame()
        
        analisis = df.groupby('Transportadora').agg(
            Total=('No_Orden', 'count'),
            Cumplen=('Cumple_NNS', lambda x: (x == 'Cumple').sum()),
            Desvio_Prom=('Desvio_Entrega', 'mean')
        ).reset_index()
        
        analisis['Pct_Cumplimiento'] = round(analisis['Cumplen'] / analisis['Total'] * 100, 1)
        analisis['Desvio_Prom'] = analisis['Desvio_Prom'].fillna(0).round(1)
        analisis = analisis.sort_values('Total', ascending=False)
        
        return analisis
    
    def get_pedidos_incumplimiento(self, df: pd.DataFrame) -> pd.DataFrame:
        """Filtra y retorna solo los pedidos con incumplimiento."""
        if 'Cumple_NNS' not in df.columns or len(df) == 0:
            return pd.DataFrame()
        
        inc = df[df['Cumple_NNS'] == 'No cumple'].copy()
        return inc
    
    def get_analisis_mes(self, df: pd.DataFrame) -> pd.DataFrame:
        """Genera análisis de tendencia mensual."""
        if 'Mes_Label' not in df.columns or 'Mes_Sort' not in df.columns or len(df) == 0:
            return pd.DataFrame()
        
        analisis = df.groupby(['Mes_Sort', 'Mes_Label']).agg(
            Total=('No_Orden', 'count'),
            Cumplen=('Cumple_NNS', lambda x: (x == 'Cumple').sum())
        ).reset_index()
        
        analisis['Pct_Cumplimiento'] = round(analisis['Cumplen'] / analisis['Total'] * 100, 1)
        analisis = analisis.sort_values('Mes_Sort')
        
        return analisis
    
    def get_recomendaciones(self, df: pd.DataFrame) -> list:
        """Genera recomendaciones automáticas basadas en los datos."""
        recs = []
        
        if len(df) == 0:
            return recs
        
        ind = self.get_indicadores(df)
        
        if ind['pct_cumplimiento'] < 70:
            recs.append((
                "🔴 Cumplimiento Crítico",
                f"El cumplimiento actual es {ind['pct_cumplimiento']}%. Revisar procesos urgentemente.",
                "error"
            ))
        elif ind['pct_cumplimiento'] < 80:
            recs.append((
                "⚠️ Cumplimiento por Debajo de Meta",
                f"El cumplimiento es {ind['pct_cumplimiento']}% (meta: 80%). Enfocar esfuerzos en reducir desvíos.",
                "warning"
            ))
        else:
            recs.append((
                "✅ Cumplimiento en Meta",
                f"El cumplimiento es {ind['pct_cumplimiento']}%. Continuar con las prácticas actuales.",
                "success"
            ))
        
        if ind['promedio_desvio_entrega'] > 5:
            recs.append((
                "⚠️ Desvíos de Entrega Elevados",
                f"El desvío promedio es {ind['promedio_desvio_entrega']} días. Evaluar capacidad de transporte.",
                "warning"
            ))
        
        if 'Area_Incumple' in df.columns:
            inc = df[df['Cumple_NNS'] == 'No cumple']
            if len(inc) > 0:
                areas = inc['Area_Incumple'].value_counts()
                if len(areas) > 0:
                    top_area = areas.index[0]
                    top_count = areas.iloc[0]
                    pct = round(top_count / len(inc) * 100, 1)
                    recs.append((
                        f"🎯 Área de Mejora: {top_area}",
                        f"El {pct}% de los incumplimientos son responsabilidad de '{top_area}'.",
                        "info"
                    ))
        
        return recs
    
    def generate_mega_report(self, df_filtrado: pd.DataFrame, ind_filtrado: dict, ind_global: dict) -> io.BytesIO:
        """Genera archivo Excel con múltiples hojas de análisis."""
        buf = io.BytesIO()
        
        with pd.ExcelWriter(buf, engine='openpyxl') as writer:
            resumen = pd.DataFrame({
                'Métrica': ['Total Pedidos', 'Cumplimiento NNS', 'Desvío Promedio Entrega'],
                'Valor Filtrado': [
                    ind_filtrado['total_pedidos'],
                    f"{ind_filtrado['pct_cumplimiento']}%",
                    f"{ind_filtrado['promedio_desvio_entrega']} días"
                ],
                'Valor Global': [
                    ind_global['total_pedidos'],
                    f"{ind_global['pct_cumplimiento']}%",
                    f"{ind_global['promedio_desvio_entrega']} días"
                ]
            })
            resumen.to_excel(writer, sheet_name='Resumen', index=False)
            df_filtrado.to_excel(writer, sheet_name='Datos', index=False)
            
            if 'Ciudad' in df_filtrado.columns:
                ciudad_analysis = self.get_analisis_ciudad(df_filtrado)
                if len(ciudad_analysis) > 0:
                    ciudad_analysis.to_excel(writer, sheet_name='Por Ciudad', index=False)
            
            if 'Transportadora' in df_filtrado.columns:
                transp_analysis = self.get_analisis_transportadora(df_filtrado)
                if len(transp_analysis) > 0:
                    transp_analysis.to_excel(writer, sheet_name='Por Transportadora', index=False)
        
        buf.seek(0)
        return buf