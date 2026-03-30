import pandas as pd
from data_processor import DataProcessor
df = pd.read_excel('Seguimiento gestion despachos TECU 2026 Indicadores.xlsx', header=3)
dp = DataProcessor(df)
proc_df = dp.procesar()
print("Value counts Cumple_NNS:")
print(proc_df['Cumple_NNS'].value_counts())
print("\nFecha_Entrega nulls:", proc_df['Fecha_Entrega'].isna().sum())
print("\nFirst 5 Fecha_Entrega:")
print(proc_df['Fecha_Entrega'].head())
