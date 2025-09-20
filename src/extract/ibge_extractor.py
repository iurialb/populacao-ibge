import requests
import pandas as pd
from typing import Dict, List
import time
from loguru import logger


class IBGEExtractor:
    def __init__(self, base_url: str, timeout: int = 30):
        self.base_url = base_url
        self.timeout = timeout

    def extract_municipios(self) -> pd.DataFrame:
        """Extrai dados de todos os municípios brasileiros"""
        endpoint = f"{self.base_url}/localidades/municipios"

        try:
            logger.info("Iniciando extração de dados dos municípios")
            response = requests.get(endpoint, timeout=self.timeout)
            response.raise_for_status()

            municipios_data = response.json()
            logger.info(f"Extraídos {len(municipios_data)} municípios")

            return pd.DataFrame(municipios_data)

        except requests.RequestException as e:
            logger.error(f"Erro na extração: {e}")
            raise

    def extract_populacao_municipios(self) -> pd.DataFrame:
        """Extrai dados de população dos municípios (estimativa)"""
        endpoint = f"{self.base_url}/projecoes/populacao/municipios"

        try:
            logger.info("Iniciando extração de dados de população")
            response = requests.get(endpoint, timeout=self.timeout)
            response.raise_for_status()

            pop_data = response.json()
            logger.info(f"Extraídos dados de população para análise")

            return pd.DataFrame(pop_data)

        except requests.RequestException as e:
            logger.error(f"Erro na extração de população: {e}")
            return pd.DataFrame()  # Retorna DataFrame vazio em caso de erro
