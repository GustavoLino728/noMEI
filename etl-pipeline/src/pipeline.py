"""
Módulo de orquestração do pipeline ETL do PNCP.

Coordena as etapas de Extração, Transformação e Carga (ETL),
incluindo separação de entidades em múltiplas coleções no MongoDB.
"""

import logging
import time
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any
from unittest import result

from config.settings import Settings
from src.extractor import PNCPExtractor
from src.transformer import PNCPTransformer
from src.loader import MongoDBLoader

logger = logging.getLogger(__name__)


@dataclass
class PipelineResult:
    """
    Resultado consolidado da execução do pipeline ETL.

    Attributes:
        total_extraido (int): Total de registros extraídos da API.
        total_transformado (int): Total de registros transformados com sucesso.
        total_inserido (int): Quantidade de documentos inseridos no MongoDB.
        total_atualizado (int): Quantidade de documentos atualizados no MongoDB.
        total_erros (int): Total de erros ocorridos durante o pipeline.
        inicio (datetime): Momento de início da execução.
        fim (datetime | None): Momento de término da execução.
    """

    total_extraido: int = 0
    total_transformado: int = 0
    total_inserido: int = 0
    total_atualizado: int = 0
    total_erros: int = 0
    inicio: datetime = datetime.now(tz=timezone.utc)
    fim: datetime | None = None

    @property
    def sucesso(self) -> bool:
        """
        Indica se o pipeline foi executado sem erros.

        Returns:
            bool: True se não houver erros, False caso contrário.
        """
        return self.total_erros == 0

    def resumo(self) -> str:
        """
        Retorna um resumo textual da execução do pipeline.

        Returns:
            str: Resumo com métricas da execução.
        """
        duracao = (self.fim - self.inicio).total_seconds() if self.fim else 0
        return (
            f"Sucesso: {self.sucesso} | Duração: {duracao:.1f}s | "
            f"Extraídos: {self.total_extraido} | "
            f"Transformados: {self.total_transformado} | "
            f"Inseridos: {self.total_inserido} | "
            f"Atualizados: {self.total_atualizado} | "
            f"Erros: {self.total_erros}"
        )


class ETLPipeline:
    """
    Orquestrador do pipeline ETL para dados do PNCP.

    Fluxo:
        1. Extract: coleta dados da API do PNCP.
        2. Transform: limpa, normaliza e estrutura os dados.
        3. Load: persiste os dados no MongoDB Atlas.

    O pipeline utiliza micro-batches e permite separação de entidades
    em múltiplas coleções (ex: contratações e órgãos).
    """

    def __init__(self, settings: Settings) -> None:
        """
        Inicializa o pipeline ETL.

        Args:
            settings (Settings): Configurações da aplicação.
        """
        self.settings = settings
        self.extractor = PNCPExtractor(settings)
        self.transformer = PNCPTransformer()
        self.loader = MongoDBLoader(settings)

    def run(self, extract_params: dict[str, Any]) -> PipelineResult:
        """
        Executa o pipeline ETL completo.

        Args:
            extract_params (dict): Parâmetros de extração enviados à API.

        Returns:
            PipelineResult: Estatísticas da execução.
        """
        logger.info("=== Iniciando pipeline ETL PNCP ===")
        logger.info("Parâmetros de extração: %s", extract_params)

        start_time = time.time()
        result = PipelineResult()

        try:
            with self.loader:
                buffer: list[dict] = []

                if "page_size" not in extract_params:
                    extract_params["page_size"] = self.settings.PAGE_SIZE
                
                for raw_record in self.extractor.extract(**extract_params):
                    result.total_extraido += 1
                    buffer.append(raw_record)
                    
                    if len(buffer) >= self.settings.BATCH_SIZE:
                        self._process_batch(buffer, result)
                        buffer.clear()

                if buffer:
                    self._process_batch(buffer, result)

        except Exception as e:
            logger.exception("Erro crítico no pipeline: %s", str(e))
            result.total_erros += 1

        result.fim = datetime.now(tz=timezone.utc)

        logger.info("Pipeline finalizado em %.2fs", time.time() - start_time)
        logger.info("Pipeline ETL PNCP | %s", result.resumo())

        return result

    def _process_batch(
        self, buffer: list[dict[str, Any]], result: PipelineResult
    ) -> None:
        """
        Processa um lote de registros (micro-batch).

        Etapas:
            - Transformação dos dados
            - Separação em múltiplas entidades
            - Carga no MongoDB

        Args:
            buffer (list[dict]): Lista de registros brutos.
            result (PipelineResult): Objeto de métricas da execução.
        """
        logger.info("Processando batch de %d registros", len(buffer))

        contratacoes = []
        orgaos = []

        for record in buffer:
            try:
                doc = self.transformer.transform(record)
                contratacoes.append(doc)

                # Separação de entidade: órgão
                orgao = doc.get("orgaoEntidade")

                if isinstance(orgao, dict):
                    orgao_doc = {
                        "_id": orgao.get("cnpj"),
                        "razaoSocial": orgao.get("razaoSocial"),
                        "uf": orgao.get("ufSigla"),
                        "_etl_ingestao_em": doc["_etl_ingestao_em"],
                    }

                    if orgao_doc["_id"]:
                        orgaos.append(orgao_doc)

            except Exception:
                logger.exception("Erro ao transformar registro")
                result.total_erros += 1

        result.total_transformado += len(contratacoes)

        summary = self.loader.load_batch(contratacoes)

        result.total_inserido += summary["inserted"]
        result.total_atualizado += summary["modified"]
        result.total_erros += summary["errors"]

        if orgaos:
            self.loader.load_into_collection(orgaos, "orgaos")