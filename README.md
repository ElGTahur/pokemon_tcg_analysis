Pokémon TCG Analytics — Proyecto ETL + Dashboard

Este proyecto realiza análisis completo de cartas del juego Pokémon TCG, desde la extracción y transformación de datos, hasta la creación de una base de datos y un dashboard interactivo construido con Streamlit.

Permite explorar:

Precios históricos y actuales de cartas

Rarezas

Expansiones

Generaciones

Estadísticas avanzadas

Distribuciones y visualizaciones dinámicas

Todo alimentado desde una base de datos SQLite generada automáticamente por un pipeline ETL.

1. Descripción del Proyecto

El proyecto sigue la arquitectura clásica ETL (Extract, Transform, Load):

✔ EXTRACT

Se extraen datos desde un archivo CSV de cartas de Pokémon TCG.

✔ TRANSFORM

Se limpian, corrigen, enriquecen y transforman los datos:

Normalización de tipos de carta

Extracción de generación

Limpieza y conversión de precios

Identificación de rareza y score de rareza

Detección y eliminación de duplicados

✔ LOAD

Los datos procesados se cargan a una base de datos SQLite, con las siguientes tablas:

cards

expansions

vw_card_details (vista)

vw_statistics (vista)

vw_prices_by_generation

vw_rarity_distribution

✔ Dashboard

Un dashboard interactivo permite:

Filtrar por precio, tipo, rareza, generación, Pokémon y expansión

Ver estadísticas generales

Visualizar distribuciones, rankings y gráficos

Exportar datos filtrados

2. Instalación de Dependencias
Requisitos:

Python 3.10+

pip

Instalación:

En la carpeta raíz del proyecto:

pip install -r requirements.txt


Si no tienes el archivo, aquí las dependencias principales:

pandas
numpy
streamlit
plotly
matplotlib
sqlite3-binary

3. Configuración de la Base de Datos

El pipeline ETL crea automáticamente la base de datos:

pokemon_cards.db


Ubicación esperada:

/pokemon_tcg_analysis/pokemon_cards.db


Si no existe, el dashboard mostrará:

Base de datos no encontrada. Ejecute el pipeline ETL.

4. Ejecutar el Pipeline ETL

Desde la raíz del proyecto:

python scripts/main_etl.py


El pipeline hará:

Leer el CSV crudo en data/raw/

Transformar los datos

Guardar datos procesados en data/processed/

Construir la base de datos SQLite

Crear tablas y vistas

Insertar más de 25 000 registros

Si todo sale bien verás:

✓ Pipeline completado exitosamente
Base de datos creada: pokemon_cards.db

 5. Ejecutar el Dashboard

Desde la carpeta raíz del proyecto:

streamlit run dashboard/app.py


Luego se abrirá en tu navegador:

http://localhost:8501


El dashboard cargará los datos desde pokemon_cards.db.

6. Estructura del Proyecto
pokemon_tcg_analysis/
│
├── data/
│   ├── raw/
│   │   └── pokemon_cards.csv
│   ├── processed/
│   │   └── pokemon_cards_clean.csv
│
├── scripts/
│   ├── extraction.py
│   ├── transformation.py
│   ├── load.py
│   └── main_etl.py
│
├── database/
│   └── schema.sql
│
├── dashboard/
│   └── app.py
│
├── pokemon_cards.db
├── requirements.txt
└── README.md

 7. Comandos Útiles
Verificar archivos modificados:
git status

Subir cambios al repositorio:
git add .
git commit -m "Update ETL and dashboard"
git push