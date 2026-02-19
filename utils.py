"""
Utilidades para cálculo de días hábiles y festivos Colombia
"""

from datetime import date, timedelta
import pandas as pd


# ─────────────────────────────────────────────
# Festivos Colombia 2024 – 2027
# ─────────────────────────────────────────────
FESTIVOS_COLOMBIA = {
    # 2024
    date(2024, 1, 1),   # Año Nuevo
    date(2024, 1, 8),   # Reyes Magos
    date(2024, 3, 25),  # Día de San José
    date(2024, 3, 28),  # Jueves Santo
    date(2024, 3, 29),  # Viernes Santo
    date(2024, 5, 1),   # Día del Trabajo
    date(2024, 5, 13),  # Ascensión del Señor
    date(2024, 6, 3),   # Corpus Christi
    date(2024, 6, 10),  # Sagrado Corazón
    date(2024, 7, 1),   # San Pedro y San Pablo
    date(2024, 7, 20),  # Independencia
    date(2024, 8, 7),   # Batalla de Boyacá
    date(2024, 8, 19),  # Asunción de la Virgen
    date(2024, 10, 14), # Día de la Raza
    date(2024, 11, 4),  # Todos los Santos
    date(2024, 11, 11), # Independencia de Cartagena
    date(2024, 12, 8),  # Inmaculada Concepción
    date(2024, 12, 25), # Navidad
    # 2025
    date(2025, 1, 1),   # Año Nuevo
    date(2025, 1, 6),   # Reyes Magos
    date(2025, 3, 24),  # Día de San José
    date(2025, 4, 17),  # Jueves Santo
    date(2025, 4, 18),  # Viernes Santo
    date(2025, 5, 1),   # Día del Trabajo
    date(2025, 6, 2),   # Corpus Christi
    date(2025, 6, 23),  # Sagrado Corazón
    date(2025, 6, 30),  # San Pedro y San Pablo
    date(2025, 7, 20),  # Independencia
    date(2025, 8, 7),   # Batalla de Boyacá
    date(2025, 8, 18),  # Asunción de la Virgen
    date(2025, 10, 13), # Día de la Raza
    date(2025, 11, 3),  # Todos los Santos
    date(2025, 11, 17), # Independencia de Cartagena
    date(2025, 12, 8),  # Inmaculada Concepción
    date(2025, 12, 25), # Navidad
    # 2026
    date(2026, 1, 1),   # Año Nuevo
    date(2026, 1, 12),  # Reyes Magos
    date(2026, 3, 23),  # Día de San José
    date(2026, 4, 2),   # Jueves Santo
    date(2026, 4, 3),   # Viernes Santo
    date(2026, 5, 1),   # Día del Trabajo
    date(2026, 5, 18),  # Ascensión del Señor
    date(2026, 6, 8),   # Corpus Christi
    date(2026, 6, 15),  # Sagrado Corazón
    date(2026, 6, 29),  # San Pedro y San Pablo
    date(2026, 7, 20),  # Independencia
    date(2026, 8, 7),   # Batalla de Boyacá
    date(2026, 8, 17),  # Asunción de la Virgen
    date(2026, 10, 12), # Día de la Raza
    date(2026, 11, 2),  # Todos los Santos
    date(2026, 11, 16), # Independencia de Cartagena
    date(2026, 12, 8),  # Inmaculada Concepción
    date(2026, 12, 25), # Navidad
    # 2027
    date(2027, 1, 1),   # Año Nuevo
    date(2027, 1, 11),  # Reyes Magos
}


# ─────────────────────────────────────────────
# Ciudades con SLA de 3 días hábiles
# ─────────────────────────────────────────────
CIUDADES_3_DIAS = [
    'bogota', 'bogotá', 'cundinamarca', 'soacha', 'bosa', 'fontibon',
    'medellin', 'medellín', 'antioquia', 'envigado', 'bello', 'itagui', 'itagüi',
    'cali', 'valle del cauca', 'palmira', 'yumbo',
]


def calcular_dias_habiles(fecha_inicio, fecha_fin):
    """
    Calcula días hábiles entre dos fechas (excluye sábados, domingos y festivos).
    Incluye ambas fechas en el conteo.
    """
    if pd.isna(fecha_inicio) or pd.isna(fecha_fin):
        return None

    if isinstance(fecha_inicio, pd.Timestamp):
        fecha_inicio = fecha_inicio.date()
    if isinstance(fecha_fin, pd.Timestamp):
        fecha_fin = fecha_fin.date()

    if fecha_fin < fecha_inicio:
        return 0

    dias_habiles = 0
    fecha_actual = fecha_inicio

    while fecha_actual <= fecha_fin:
        if fecha_actual.weekday() < 5 and fecha_actual not in FESTIVOS_COLOMBIA:
            dias_habiles += 1
        fecha_actual += timedelta(days=1)

    return dias_habiles


def determinar_sla_entrega(ciudad, principal_val=3, other_val=5):
    """
    SLA según ciudad:
      - principal_val (ej 3) → Bogotá, Medellín, Cali y alrededores
      - other_val (ej 5) → Todas las demás
    """
    if pd.isna(ciudad):
        return other_val

    ciudad_str = str(ciudad).strip().lower()
    # Eliminar tildes comunes para comparación robusta
    ciudad_norm = (ciudad_str
                   .replace('á', 'a').replace('é', 'e')
                   .replace('í', 'i').replace('ó', 'o').replace('ú', 'u'))

    for c in CIUDADES_3_DIAS:
        c_norm = (c.replace('á', 'a').replace('é', 'e')
                   .replace('í', 'i').replace('ó', 'o').replace('ú', 'u'))
        if c_norm in ciudad_norm:
            return principal_val
    return other_val


def determinar_area_incumple(desvio_despacho, desvio_entrega, transportadora):
    """
    Determina el área responsable del incumplimiento.
    - Almacén/Logística interna → demora en despacho
    - Transporte → demora en tránsito (entrega)
    - Mixto → ambas áreas
    """
    tiene_desvio_despacho = pd.notna(desvio_despacho) and desvio_despacho > 0
    tiene_desvio_entrega = pd.notna(desvio_entrega) and desvio_entrega > 0

    if not tiene_desvio_despacho and not tiene_desvio_entrega:
        return 'N/A'

    if tiene_desvio_despacho and tiene_desvio_entrega:
        return 'Mixto (Almacén + Transporte)'
    elif tiene_desvio_despacho:
        return 'Almacén/Logística'
    else:
        # Solo demora en entrega = responsabilidad del transportador
        transp = str(transportadora).strip() if pd.notna(transportadora) else 'Transporte'
        return f'Transportadora ({transp})'


def evaluar_cumple_nns(desvio_entrega):
    """Evalúa cumplimiento NNS basado en desvío de entrega."""
    if pd.isna(desvio_entrega):
        return 'PTE'
    return 'Cumple' if desvio_entrega <= 0 else 'No cumple'