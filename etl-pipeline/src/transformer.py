"""
Módulo de transformação de dados do PNCP.

Responsável por limpar, normalizar e enriquecer os registros brutos
retornados pela API, preparando-os para persistência no MongoDB.
"""

import logging
from datetime import datetime, timezone
from typing import Any

logger = logging.getLogger(__name__)

# Campos de data
_DATE_FIELDS = (
    "dataAtualizacao",
    "dataInclusao",
    "dataAberturaProposta",
    "dataEncerramentoProposta",
    "dataPublicacaoPncp",
    "dataAtualizacaoGlobal",
)

# Formatos aceitos
_DATE_FORMATS = (
    "%Y-%m-%dT%H:%M:%S",
    "%Y-%m-%dT%H:%M:%S.%f",
    "%Y-%m-%d",
)


def _parse_date(value: str | None) -> datetime | None:
    """Converte string para datetime UTC."""
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


def _clean_dict(data: dict[str, Any]) -> dict[str, Any]:
    """
    Remove valores inválidos do dicionário:
    - None
    - strings vazias
    - listas vazias
    """
    return {
        k: v
        for k, v in data.items()
        if v is not None and v != "" and v != []
    }


class PNCPTransformer:
    """
    Transforma registros brutos da API do PNCP em documentos limpos para o MongoDB.

    Operações:
    - Conversão de datas
    - Normalização de texto
    - Limpeza de dados
    - Definição de chave única (_id)
    """

    def transform(self, record: dict[str, Any]) -> dict[str, Any]:
        """
        Transforma um registro bruto.

        Args:
            record (dict): Registro vindo da API.

        Returns:
            dict: Documento pronto para o MongoDB.
        """

        doc = dict(record)

        # 1. Converter datas
        for field in _DATE_FIELDS:
            if field in doc:
                doc[field] = _parse_date(doc[field])

        # 2. Normalização de texto
        text_fields = (
            "objetoCompra",
            "informacaoComplementar",
            "justificativaPresencial",
        )

        for field in text_fields:
            if isinstance(doc.get(field), str):
                doc[field] = doc[field].strip()

        # 3. Normalização do órgão
        orgao = doc.get("orgaoEntidade")
        if isinstance(orgao, dict):
            orgao["razaoSocial"] = (
                (orgao.get("razaoSocial") or "")
                .strip()
                .upper()
            )

        # 4. Limpeza geral
        doc = _clean_dict(doc)

        # 5. Garantir _id
        numero_controle = doc.get("numeroControlePNCP")
        if not numero_controle:
            raise ValueError("Registro sem numeroControlePNCP")

        doc["_id"] = numero_controle

        # 6. Metadata ETL
        doc["_etl_ingestao_em"] = datetime.now(tz=timezone.utc)

        return doc

    def transform_batch(
        self, records: list[dict[str, Any]]
    ) -> list[dict[str, Any]]:
        """
        Transforma lista de registros.

        Args:
            records (list): Registros brutos.

        Returns:
            list: Registros transformados.
        """

        transformed = []

        for idx, record in enumerate(records):
            try:
                transformed.append(self.transform(record))
            except Exception:
                logger.exception(
                    "Erro ao transformar registro %d (numeroControlePNCP=%s)",
                    idx,
                    record.get("numeroControlePNCP", "desconhecido"),
                )

        return transformed