import pandas as pd
import sqlite3
from pathlib import Path
from loguru import logger
from sqlalchemy import create_engine
import json


class DataLoader:
    def __init__(self, db_path: str):
        self.db_path = Path(db_path)
        self.db_path.parent.mkdir(parents=True, exist_ok=True)

    def load_to_sqlite(self, df: pd.DataFrame, table_name: str, if_exists: str = 'replace'):
        """Carrega dados no SQLite"""
        try:
            engine = create_engine(f'sqlite:///{self.db_path}')

            logger.info(
                f"Carregando {len(df)} registros na tabela {table_name}")
            df.to_sql(table_name, engine, if_exists=if_exists, index=False)

            logger.info(f"Dados carregados com sucesso em {table_name}")

        except Exception as e:
            logger.error(f"Erro ao carregar dados: {e}")
            raise

    def save_to_csv(self, df: pd.DataFrame, file_path: str):
        """Salva dados em CSV"""
        try:
            file_path = Path(file_path)
            file_path.parent.mkdir(parents=True, exist_ok=True)

            df.to_csv(file_path, index=False, encoding='utf-8')
            logger.info(f"Dados salvos em CSV: {file_path}")

        except Exception as e:
            logger.error(f"Erro ao salvar CSV: {e}")
            raise

    def save_validation_report(self, report: dict, file_path: str):
        """Salva relatório de validação"""
        try:
            file_path = Path(file_path)
            file_path.parent.mkdir(parents=True, exist_ok=True)

            with open(file_path, 'w', encoding='utf-8') as f:
                json.dump(report, f, indent=2, ensure_ascii=False)

            logger.info(f"Relatório salvo: {file_path}")

        except Exception as e:
            logger.error(f"Erro ao salvar relatório: {e}")
            raise
