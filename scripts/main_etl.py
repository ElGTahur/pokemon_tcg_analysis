"""
Script principal del pipeline ETL para Pokémon TCG
Ejecuta todo el proceso de Extracción, Transformación y Carga
"""

import logging
import sys
from pathlib import Path
from datetime import datetime

# Agregar el directorio actual al path para importar módulos
sys.path.append(str(Path(__file__).parent))

# Configuración de logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('etl_pipeline.log'),
        logging.StreamHandler(sys.stdout)
    ]
)
logger = logging.getLogger(__name__)

def run_etl_pipeline():
    """
    Ejecuta el pipeline ETL completo
    """
    start_time = datetime.now()
    logger.info("=" * 60)
    logger.info("INICIANDO PIPELINE ETL - POKÉMON TCG")
    logger.info(f"Fecha y hora: {start_time}")
    logger.info("=" * 60)
    
    try:
        # Importar módulos
        from extraction import extract_data, save_raw_data
        from transformation import transform_data, save_transformed_data
        from load import load_data_to_db
        
        # PASO 1: EXTRACCIÓN
        logger.info("\n" + "=" * 60)
        logger.info("PASO 1: EXTRACCIÓN DE DATOS")
        logger.info("=" * 60)
        
        df_raw = extract_data()
        save_raw_data(df_raw)
        
        # PASO 2: TRANSFORMACIÓN
        logger.info("\n" + "=" * 60)
        logger.info("PASO 2: TRANSFORMACIÓN DE DATOS")
        logger.info("=" * 60)
        
        df_transformed = transform_data(df_raw)
        save_transformed_data(df_transformed)
        
        # PASO 3: CARGA
        logger.info("\n" + "=" * 60)
        logger.info("PASO 3: CARGA A BASE DE DATOS")
        logger.info("=" * 60)
        
        results = load_data_to_db(df_transformed)
        
        # RESUMEN FINAL
        end_time = datetime.now()
        duration = end_time - start_time
        
        logger.info("\n" + "=" * 60)
        logger.info("PIPELINE ETL COMPLETADO EXITOSAMENTE")
        logger.info("=" * 60)
        logger.info(f"Duración total: {duration}")
        logger.info(f"Cartas procesadas: {results['cards_loaded']}")
        logger.info(f"Expansiones procesadas: {results['expansions_loaded']}")
        logger.info(f"Timestamp: {results['timestamp']}")
        logger.info("=" * 60)
        
        return {
            "status": "success",
            "duration": str(duration),
            "results": results,
            "timestamp": end_time.isoformat()
        }
        
    except Exception as e:
        logger.error("\n" + "=" * 60)
        logger.error("ERROR EN EL PIPELINE ETL")
        logger.error("=" * 60)
        logger.error(f"Error: {str(e)}", exc_info=True)
        logger.error("=" * 60)
        
        return {
            "status": "error",
            "error": str(e),
            "timestamp": datetime.now().isoformat()
        }

def main():
    """
    Función principal
    """
    print("Pokémon TCG ETL Pipeline")
    print("-" * 30)
    print("Este pipeline realizará:")
    print("1. Extracción de datos del archivo CSV")
    print("2. Transformación y limpieza de datos")
    print("3. Carga a base de datos SQLite")
    print("-" * 30)
    
    response = input("¿Desea ejecutar el pipeline? (s/n): ").strip().lower()
    
    if response == 's':
        print("\nIniciando pipeline ETL...")
        results = run_etl_pipeline()
        
        if results["status"] == "success":
            print("\n✓ Pipeline completado exitosamente!")
            print(f"   Duración: {results['duration']}")
            print(f"   Cartas procesadas: {results['results']['cards_loaded']}")
            print(f"   Base de datos creada: pokemon_cards.db")
        else:
            print("\n✗ Error en el pipeline")
            print(f"   Error: {results['error']}")
            print("   Revise el archivo etl_pipeline.log para más detalles")
    else:
        print("\nPipeline cancelado.")

if __name__ == "__main__":
    main()