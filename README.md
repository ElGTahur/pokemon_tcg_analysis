#  Pokémon TCG Analytics - Proyecto ETL con Dashboard

![Python](https://img.shields.io/badge/Python-3.8%2B-blue)
![Streamlit](https://img.shields.io/badge/Streamlit-1.28%2B-red)
![SQLite](https://img.shields.io/badge/SQLite-3.0%2B-green)
![Pandas](https://img.shields.io/badge/Pandas-1.5%2B-orange)

Un pipeline ETL completo para análisis de cartas de Pokémon TCG, con dashboard interactivo en Streamlit.

## Contenido

- [Descripción del Proyecto](#-descripción-del-proyecto)
- [Características](#-características)
- [Estructura del Proyecto](#-estructura-del-proyecto)
- [Instalación](#-instalación)
- [Uso](#-uso)
- [Pipeline ETL](#-pipeline-etl)
- [Dashboard](#-dashboard)
- [Base de Datos](#-base-de-datos)
- [Ejemplos de Consultas](#-ejemplos-de-consultas)
- [Capturas de Pantalla](#-capturas-de-pantalla)
- [Tecnologías Utilizadas](#-tecnologías-utilizadas)
- [Contribución](#-contribución)
- [Licencia](#-licencia)

##  Descripción del Proyecto

Este proyecto implementa un pipeline ETL (Extracción, Transformación y Carga) completo para analizar el valor de cartas de Pokémon TCG. El sistema procesa datos de más de 2,500 cartas, las carga en una base de datos SQLite y proporciona un dashboard interactivo para visualización y análisis.

**Caso de uso:** Coleccionistas e inversores de cartas Pokémon pueden usar este dashboard para:
- Analizar tendencias de precios
- Identificar cartas de mayor valor
- Comparar diferentes versiones
- Tomar decisiones informadas sobre compras/ventas

##  Características

### Pipeline ETL
-  Extracción automática de datos CSV
-  Limpieza y transformación de datos
-  Carga a base de datos SQLite
-  Validación y logging completo
-  Manejo de errores robusto

### Dashboard
-  4 visualizaciones interactivas
-  6 filtros diferentes
-  Métricas en tiempo real
-  Exportación de datos
-  Análisis estadístico avanzado

### Base de Datos
-  Esquema normalizado
-  Índices optimizados
-  Vistas precalculadas
-  Triggers para integridad
-  Metadatos de ejecución

##  Estructura del Proyecto

pokemon_tcg_analysis/
│
├── data/
│ ├── raw/ # Datos crudos
│ │ └── pokemon_cards.csv
│ └── processed/ # Datos transformados
│ └── pokemon_cards_clean.csv
│
├── scripts/ # Pipeline ETL
│ ├── extraction.py
│ ├── transformation.py
│ ├── load.py
│ └── main_etl.py
│
├── database/ # Esquema BD
│ └── schema.sql
│
├── dashboard/ # Dashboard Streamlit
│ └── app.py
│
├── requirements.txt # Dependencias
├── pokemon_cards.db # Base de datos (generada)
└── README.me


## Instalación

### 1. Clonar el repositorio
```bash
git clone <repo-url>
cd pokemon_tcg_analysis

2. Crear entorno virtual (opcional pero recomendado)
bash

python -m venv venv
source venv/bin/activate  # En Windows: venv\Scripts\activate

3. Instalar dependencias
bash

pip install -r requirements.txt

Uso
Ejecutar el pipeline ETL completo
bash

python scripts/main_etl.py

Esto ejecutará:

    Extracción: Lee el archivo CSV original

    Transformación: Limpia y procesa los datos

    Carga: Crea la base de datos SQLite e inserta los datos

Ejecutar el dashboard
bash

streamlit run dashboard/app.py

El dashboard se abrirá automáticamente en tu navegador (usualmente en http://localhost:8501).
Pipeline ETL
Paso 1: Extracción
python

# Lee el archivo CSV con encoding UTF-8
# Maneja el símbolo de libra 'Ł'
# Valida la estructura del archivo

Paso 2: Transformación

    Limpia nombres de Pokémon y expansiones

    Normaliza tipos de carta

    Calcula niveles de rareza

    Extrae información de números de carta

    Maneja valores nulos y duplicados

Paso 3: Carga

    Crea base de datos SQLite

    Inserta datos en tablas normalizadas

    Crea índices y vistas

    Valida la integridad de los datos

Dashboard
Filtros disponibles:

     Rango de precios - Deslizador para filtrar por precio

     Tipo de carta - Selector múltiple (Standard, Reverse Holo, etc.)

     Generación - Dropdown con diferentes generaciones

     Nivel de rareza - Selector múltiple (Common, Rare, etc.)

     Expansión - Dropdown para filtrar por expansión específica

     Buscar Pokémon - Búsqueda por nombre

Visualizaciones:

    Distribución de precios - Histograma interactivo

     Top Pokémon más caros - Gráfico de barras horizontal

     Precios por tipo - Box plot comparativo

     Análisis por generación - Gráficos de barras y pie

Funcionalidades adicionales:

     Descarga de datos filtrados en CSV

     Análisis de correlación precio-raridad

     Estadísticas descriptivas

     Actualización en tiempo real

 Base de Datos
Esquema principal:
sql

expansions (expansion_id, name, generation)
cards (card_id, expansion_id, pokemon_name, card_type, price, rarity_level, ...)

Vistas precalculadas:

    vw_card_details - Detalles completos de cartas

    vw_statistics - Estadísticas generales

    vw_prices_by_generation - Análisis por generación

    vw_rarity_distribution - Distribución de rareza

 Ejemplos de Consultas
Top 10 Pokémon más caros:
sql

SELECT pokemon_name, price, rarity_level, expansion_name
FROM vw_card_details
ORDER BY price DESC
LIMIT 10;

Precio promedio por generación:
sql

SELECT generation, AVG(price) as avg_price, COUNT(*) as card_count
FROM vw_card_details
GROUP BY generation
ORDER BY avg_price DESC;

Distribución de rareza:
sql

SELECT rarity_level, COUNT(*) as count,
       ROUND(COUNT(*) * 100.0 / (SELECT COUNT(*) FROM cards), 2) as percentage
FROM cards
GROUP BY rarity_level
ORDER BY count DESC;

 Tecnologías Utilizadas

    Python 3.8+ - Lenguaje principal

    Pandas - Procesamiento y análisis de datos

    Streamlit - Framework para dashboard web

    SQLite - Base de datos ligera

    Plotly - Visualizaciones interactivas

    SQLAlchemy - ORM para base de datos

    Matplotlib - Gráficos adicionales

 Contribución

    Fork el repositorio

    Crea una rama para tu feature (git checkout -b feature/AmazingFeature)

    Commit tus cambios (git commit -m 'Add some AmazingFeature')

    Push a la rama (git push origin feature/AmazingFeature)

    Abre un Pull Request

Licencia

Este proyecto es para fines educativos y de demostración. Los datos de Pokémon son propiedad de The Pokémon Company.