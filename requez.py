
import pandas as pd
import numpy as np
from datetime import datetime, timedelta
import warnings
warnings.filterwarnings('ignore')

# Cargar el archivo Excel
file_path = '/mnt/kimi/upload/Seguimiento gestion despachos TECU Aura.xlsx'

# Ver las hojas disponibles
xl = pd.ExcelFile(file_path)
print("Hojas disponibles:", xl.sheet_names)
print()

# Cargar la hoja de Base Ventas
df = pd.read_excel(file_path, sheet_name='Base Ventas', header=2)  # Header en fila 3 (índice 2)

print("=" * 80)
print("INFORMACIÓN GENERAL DE BASE VENTAS")
print("=" * 80)
print(f"Total de registros: {len(df)}")
print(f"Total de columnas: {len(df.columns)}")
print()

# Mostrar nombres de columnas
print("COLUMNAS EN EL ARCHIVO:")
for i, col in enumerate(df.columns, 1):
    print(f"{i:2d}. {col}")
