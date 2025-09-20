import yaml
from pathlib import Path
from loguru import logger
from datetime import datetime

from src.extract.ibge_extractor import IBGEExtractor
from src.transform.data_transformer import DataTransformer
from src.load.data_loader import DataLoader


class ETLPipeline:
    def __init__(self, config_path: str):
        with open(config_path, 'r', encoding='utf-8') as f:
            self.config = yaml.safe_load(f)

        self.setup_logging()

        # Inicializar componentes
        self.extractor = IBGEExtractor(
            base_url=self.config['api']['ibge_base_url'],
            timeout=self.config['api']['timeout']
        )

        self.transformer = DataTransformer()

        self.loader = DataLoader(
            db_path=self.config['database']['sqlite_path']
        )

    def setup_logging(self):
        """Configura logging"""
        log_path = Path(self.config['logging']['file'])
        log_path.parent.mkdir(parents=True, exist_ok=True)

        logger.add(
            log_path,
            level=self.config['logging']['level'],
            format="{time:YYYY-MM-DD HH:mm:ss} | {level} | {message}",
            rotation="1 day"
        )

    def run_pipeline(self):
        """Executa o pipeline completo"""
        start_time = datetime.now()
        logger.info("=== INICIANDO PIPELINE ETL ===")

        try:
            # EXTRACT
            logger.info("Fase de EXTRAÇÃO")
            raw_data = self.extractor.extract_municipios()

            # Salvar dados brutos
            self.loader.save_to_csv(raw_data, "data/raw/municipios_raw.csv")

            # TRANSFORM
            logger.info("Fase de TRANSFORMAÇÃO")
            cleaned_data = self.transformer.clean_municipios_data(raw_data)

            # Validar dados
            if self.config['processing']['validate_data']:
                validation_report = self.transformer.validate_data(
                    cleaned_data)
                self.loader.save_validation_report(
                    validation_report,
                    "data/processed/validation_report.json"
                )

            # Salvar dados processados
            self.loader.save_to_csv(
                cleaned_data, "data/processed/municipios_processed.csv")

            # LOAD
            logger.info("Fase de CARREGAMENTO")
            self.loader.load_to_sqlite(cleaned_data, "municipios")

            # Estatísticas finais
            end_time = datetime.now()
            duration = end_time - start_time

            logger.info("=== PIPELINE CONCLUÍDO COM SUCESSO ===")
            logger.info(f"Duração: {duration}")
            logger.info(f"Registros processados: {len(cleaned_data)}")

        except Exception as e:
            logger.error(f"Erro no pipeline: {e}")
            raise
