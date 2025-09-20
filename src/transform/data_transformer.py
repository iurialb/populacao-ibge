import pandas as pd
from typing import Dict, Any
from loguru import logger
import re


class DataTransformer:
    def __init__(self):
        pass

    def clean_municipios_data(self, df: pd.DataFrame) -> pd.DataFrame:
        """Limpa e padroniza dados dos municípios"""
        logger.info("Iniciando limpeza dos dados de municípios")

        # Criar DataFrame limpo
        cleaned_df = pd.DataFrame()

        try:
            # Extrair campos aninhados
            cleaned_df['municipio_id'] = df['id']
            cleaned_df['municipio_nome'] = df['nome'].str.strip().str.title()

            # Extrair dados do estado
            cleaned_df['uf_sigla'] = df['microrregiao'].apply(
                lambda x: x['mesorregiao']['UF']['sigla'] if isinstance(
                    x, dict) else None
            )
            cleaned_df['uf_nome'] = df['microrregiao'].apply(
                lambda x: x['mesorregiao']['UF']['nome'] if isinstance(
                    x, dict) else None
            )

            # Extrair região
            cleaned_df['regiao_nome'] = df['microrregiao'].apply(
                lambda x: x['mesorregiao']['UF']['regiao']['nome'] if isinstance(
                    x, dict) else None
            )

            # Dados da microrregião
            cleaned_df['microrregiao_nome'] = df['microrregiao'].apply(
                lambda x: x['nome'] if isinstance(x, dict) else None
            )

            # Dados da mesorregião
            cleaned_df['mesorregiao_nome'] = df['microrregiao'].apply(
                lambda x: x['mesorregiao']['nome'] if isinstance(
                    x, dict) else None
            )

            # Limpeza de dados
            cleaned_df = cleaned_df.dropna(
                subset=['municipio_id', 'municipio_nome'])
            cleaned_df = cleaned_df.drop_duplicates(subset=['municipio_id'])

            logger.info(f"Dados limpos: {len(cleaned_df)} registros válidos")

            return cleaned_df

        except Exception as e:
            logger.error(f"Erro na limpeza dos dados: {e}")
            raise

    def validate_data(self, df: pd.DataFrame) -> Dict[str, Any]:
        """Valida qualidade dos dados"""
        validation_report = {
            'total_records': len(df),
            'null_counts': df.isnull().sum().to_dict(),
            'duplicates': df.duplicated().sum(),
            'unique_states': df['uf_sigla'].nunique() if 'uf_sigla' in df.columns else 0
        }

        logger.info(f"Relatório de validação: {validation_report}")
        return validation_report
