# 🐉 Pokémon TCG Analytics - Proyecto ETL con Dashboard

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
