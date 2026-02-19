# Análisis de Despachos TECU Aura

Aplicación Streamlit para análisis de tiempos de entrega y cumplimiento de SLAs.

## Características

- ✅ Cálculo de días hábiles (excluye sábados, domingos y festivos colombianos)
- ✅ SLA automático: 3 días (Bogotá, Medellín, Cali) / 5 días (otras ciudades)
- ✅ Identificación de desvíos en despacho y entrega
- ✅ Determinación de áreas responsables
- ✅ Dashboard interactivo con filtros
- ✅ Exportación a Excel

## Instalación

```bash
# Clonar o crear carpeta
mkdir analisis-despachos-tecu
cd analisis-despachos-tecu

# Crear entorno virtual (opcional pero recomendado)
python -m venv venv

# Activar entorno
# Windows:
venv\Scripts\activate
# Mac/Linux:
source venv/bin/activate

# Instalar dependencias
pip install -r requirements.txt