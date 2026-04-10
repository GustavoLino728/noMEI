"""
Módulo de transformação de dados do PNCP.

Responsável por limpar, normalizar e enriquecer os registros brutos
retornados pela API, preparando-os para persistência no banco de dados.
"""

import logging
from datetime import datetime, timezone
from pydoc import doc
from typing import Any

logger = logging.getLogger(__name__)

# Campos de data que precisam ser convertidos para objetos datetime
_DATE_FIELDS = (
    "dataAtualizacao",
    "dataInclusao",
    "dataAberturaProposta",
    "dataEncerramentoProposta",
    "dataPublicacaoPncp",
    "dataAtualizacaoGlobal",
)

# Formatos de data aceitos pela API
_DATE_FORMATS = (
    "%Y-%m-%dT%H:%M:%S",
    "%Y-%m-%dT%H:%M:%S.%f",
    "%Y-%m-%d",
)


def _parse_date(value: str | None) -> datetime | None:
    """
    Converte uma string de data para objeto datetime com fuso UTC.

    Args:
        value (str | None): String de data no formato ISO.

    Returns:
        datetime | None: Objeto datetime (UTC) ou None se inválido/ausente.
    """
    if not value:
        return None
    for fmt in _DATE_FORMATS:
        try:
            dt = datetime.strptime(value, fmt)
            return dt.replace(tzinfo=timezone.utc)
        except ValueError:
            continue
    logger.warning("Formato de data não reconhecido: '%s'", value)
    return None


class PNCPTransformer:
    """
    Transforma registros brutos da API do PNCP em documentos prontos para persistência.

    Operações realizadas:
    - Conversão de campos de data para objetos datetime (UTC).
    - Remoção de campos nulos desnecessários para reduzir payload.
    - Adição de metadados de ingestão (``_etl_ingestao_em``).
    - Normalização de strings (strip de espaços).
    """

    def transform(self, record: dict[str, Any]) -> dict[str, Any]:
        """
        Transforma um único registro bruto da API.

        Args:
            record (dict): Registro bruto retornado pelo extrator.

        Returns:
            dict: Documento transformado e enriquecido, pronto para o MongoDB.

        Raises:
            ValueError: Se o registro não possuir numeroControlePNCP.
        """
        doc = dict(record)

        # 1. Converte campos de data para datetime
        for field in _DATE_FIELDS:
            if field in doc:
                doc[field] = _parse_date(doc[field])

        # 2. Normaliza strings de texto livre
        for field in (
            "objetoCompra",
            "informacaoComplementar",
            "justificativaPresencial",
        ):
            if isinstance(doc.get(field), str):
                doc[field] = doc[field].strip()

        # 3. Normaliza subdocumento orgaoEntidade
        orgao = doc.get("orgaoEntidade")
        if isinstance(orgao, dict):
            orgao["razaoSocial"] = (orgao.get("razaoSocial") or "").strip().upper()

        # 4. Remove explicitamente campos None de nível raiz
        doc = {k: v for k, v in doc.items() if v is not None}

        # 5. Garante chave única obrigatória para deduplicação
        numero_controle = doc.get("numeroControlePNCP")
        if not numero_controle:
            raise ValueError("Registro sem numeroControlePNCP")

        doc["_id"] = numero_controle

        # 6. Adiciona metadado de ingestão
        doc["_etl_ingestao_em"] = datetime.now(tz=timezone.utc)

        return doc

    def transform_batch(
        self, records: list[dict[str, Any]]
    ) -> list[dict[str, Any]]:
        """
        Transforma uma lista de registros brutos.

        Args:
            records (list[dict]): Lista de registros brutos.

        Returns:
            list[dict]: Lista de documentos transformados.
        """
        transformed = []

        for idx, record in enumerate(records):
            try:
                transformed.append(self.transform(record))
            except Exception:
                logger.exception(
                    "Erro ao transformar registro %d (numeroControlePNCP=%s). Ignorando.",
                    idx,
                    record.get("numeroControlePNCP", "desconhecido"),
                )
                
        return transformed