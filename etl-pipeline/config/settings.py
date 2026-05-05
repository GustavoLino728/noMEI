"""
Módulo de configurações centralizadas da aplicação ETL.

Carrega variáveis de ambiente (via arquivo .env) e expõe
parâmetros de execução com valores padrão seguros.
"""

import os
from dataclasses import dataclass

from dotenv import load_dotenv

load_dotenv()


@dataclass
class Settings:
    """
    Configurações da aplicação ETL do PNCP.

    Todas as credenciais sensíveis devem ser definidas via variáveis
    de ambiente (arquivo `.env`), nunca hard-coded no código-fonte.

    Attributes:
        BASE_URL (str): URL base da API do PNCP.
        REQUEST_TIMEOUT (int): Timeout em segundos para requisições HTTP.
        MAX_RETRIES (int): Número máximo de retentativas em caso de erro.
        RETRY_BACKOFF (float): Fator de espera entre retentativas (segundos).
        PAGE_SIZE (int): Número de registros por página (mín. 10, máx. 500).
        MAX_PAGES (int): Limite máximo de páginas a serem extraídas (proteção contra sobrecarga).
        BATCH_SIZE (int): Tamanho do micro-lote para carga no MongoDB.
        MONGODB_URI (str): URI de conexão ao MongoDB Atlas (com credenciais).
        MONGODB_DATABASE (str): Nome do banco de dados no MongoDB.
        MONGODB_COLLECTION (str): Nome da coleção principal.
    """

    # API PNCP
    BASE_URL: str = os.getenv("PNCP_BASE_URL", "https://pncp.gov.br/api/consulta")
    REQUEST_TIMEOUT: int = int(os.getenv("REQUEST_TIMEOUT", "30"))
    MAX_RETRIES: int = int(os.getenv("MAX_RETRIES", "3"))
    RETRY_BACKOFF: float = float(os.getenv("RETRY_BACKOFF", "2.0"))
    PAGE_SIZE: int = int(os.getenv("PAGE_SIZE", "50"))
    MAX_PAGES: int = int(os.getenv("MAX_PAGES", "50"))

    # ETL
    BATCH_SIZE: int = int(os.getenv("BATCH_SIZE", "100"))

    # MongoDB Atlas
    MONGODB_URI: str = os.getenv("MONGODB_URI", "")
    MONGODB_DATABASE: str = os.getenv("MONGODB_DATABASE", "pncp")
    MONGODB_COLLECTION: str = os.getenv("MONGODB_COLLECTION", "contratacoes_proposta")

    def validate(self) -> None:
        """
        Valida que as configurações obrigatórias estão presentes e corretas.

        Raises:
            ValueError: Se alguma configuração obrigatória estiver ausente ou inválida.
        """
        if not self.MONGODB_URI:
            raise ValueError(
                "MONGODB_URI não configurada. Defina no arquivo .env"
            )
        if not (10 <= self.PAGE_SIZE <= 500):
            raise ValueError("PAGE_SIZE deve estar entre 10 e 500.")
        if self.MAX_PAGES < 1:
            raise ValueError("MAX_PAGES deve ser pelo menos 1.")
        if self.MAX_RETRIES < 1:
            raise ValueError("MAX_RETRIES deve ser pelo menos 1.")
        if self.REQUEST_TIMEOUT < 5:
            raise ValueError("REQUEST_TIMEOUT deve ser maior que 5 segundos.")
        if self.BATCH_SIZE < 1:
            raise ValueError("BATCH_SIZE deve ser pelo menos 1.")