"""
Módulo de extração de dados da API pública do PNCP.

Responsável por realizar requisições HTTP paginadas ao endpoint
/v1/contratacoes/proposta e retornar os registros brutos.
"""

import logging
import time
from typing import Any, Generator

import requests

from config.settings import Settings

logger = logging.getLogger(__name__)


class PNCPExtractor:
    """
    Extrai contratações com recebimento de propostas abertas da API do PNCP.

    Suporta paginação automática e retentativas em caso de falhas transitórias.

    Attributes:
        settings (Settings): Configurações da aplicação.
        session (requests.Session): Sessão HTTP reutilizável para eficiência de rede.
    """

    _ENDPOINT = "/v1/contratacoes/proposta"

    def __init__(self, settings: Settings) -> None:
        """
        Inicializa o extrator com as configurações fornecidas.

        Args:
            settings (Settings): Objeto de configurações com base_url, timeouts, etc.
        """
        self.settings = settings
        self.session = requests.Session()
        self.session.headers.update({"Accept": "application/json"})

    def _fetch_page(self, params: dict[str, Any]) -> dict[str, Any]:
        """
        Requisita uma única página da API com retentativas.

        Args:
            params (dict): Parâmetros de query string da requisição.

        Returns:
            dict: JSON de resposta da API.

        Raises:
            requests.RequestException: Se a requisição falhar após retentativas.
        """
        url = self.settings.BASE_URL + self._ENDPOINT

        for attempt in range(1, self.settings.MAX_RETRIES + 1):
            try:
                response = self.session.get(
                    url,
                    params=params,
                    timeout=self.settings.REQUEST_TIMEOUT,
                )
                response.raise_for_status()
                return response.json()

            except requests.HTTPError as exc:
                status = exc.response.status_code if exc.response else None

                # Não faz retry para erros de cliente (4xx)
                if status and 400 <= status < 500:
                    logger.error("Erro de cliente %s ao acessar %s", status, url)
                    raise

                if attempt < self.settings.MAX_RETRIES:
                    wait = self.settings.RETRY_BACKOFF * attempt
                    logger.warning(
                        "Tentativa %d/%d falhou (HTTP %s). Aguardando %.1fs...",
                        attempt,
                        self.settings.MAX_RETRIES,
                        status,
                        wait,
                    )
                    time.sleep(wait)
                else:
                    logger.error("Falha definitiva após erro HTTP: %s", status)
                    raise

            except requests.RequestException as exc:
                if attempt < self.settings.MAX_RETRIES:
                    wait = self.settings.RETRY_BACKOFF * attempt
                    logger.warning(
                        "Tentativa %d/%d falhou (%s). Aguardando %.1fs...",
                        attempt,
                        self.settings.MAX_RETRIES,
                        str(exc),
                        wait,
                    )
                    time.sleep(wait)
                else:
                    logger.error(
                        "Falha definitiva ao acessar API do PNCP: %s",
                        str(exc),
                    )
                    raise
        return {}


    def extract(
        self,
        data_final: str,
        data_inicial: str | None = None,
        uf: str | None = None,
        codigo_modalidade: int | None = None,
        cnpj: str | None = None,
        codigo_municipio_ibge: str | None = None,
        codigo_unidade_administrativa: str | None = None,
    ) -> Generator[dict[str, Any], None, None]:
        """
        Extrai todos os registros do endpoint de propostas via paginação automática.

        Args:
            data_final (str): Data final de encerramento de propostas (formato YYYYMMDD). Obrigatório.
            data_inicial (str | None): Data inicial de encerramento de propostas (formato YYYYMMDD).
            uf (str | None): Sigla da UF para filtrar resultados.
            codigo_modalidade (int | None): Código da modalidade de contratação.
            cnpj (str | None): CNPJ do órgão contratante.
            codigo_municipio_ibge (str | None): Código IBGE do município.
            codigo_unidade_administrativa (str | None): Código da unidade administrativa.

        Yields:
            dict: Registro individual de contratação/proposta (dados brutos da API).
        """
        params: dict[str, Any] = {
            "dataFinal": data_final,
            "pagina": 1,
            "tamanhoPagina": self.settings.PAGE_SIZE,
        }
        if data_inicial:
            params["dataInicial"] = data_inicial
        if uf:
            params["uf"] = uf
        if codigo_modalidade is not None:
            params["codigoModalidadeContratacao"] = codigo_modalidade
        if cnpj:
            params["cnpj"] = cnpj
        if codigo_municipio_ibge:
            params["codigoMunicipioIbge"] = codigo_municipio_ibge
        if codigo_unidade_administrativa:
            params["codigoUnidadeAdministrativa"] = codigo_unidade_administrativa

        total_extraido = 0
        while True:
            logger.info("Buscando página %d...", params["pagina"])
            payload = self._fetch_page(params)

            records: list[dict] = payload.get("data", [])
            if not records:
                logger.info("Nenhum registro retornado. Extração concluída.")
                break

            for record in records:
                yield record
            total_extraido += len(records)

            # Verifica se há próxima página via totalRegistros
            total_registros = payload.get("totalRegistros", 0)
            pagina_atual = params["pagina"]
            max_paginas = (
                (total_registros + self.settings.PAGE_SIZE - 1)
                // self.settings.PAGE_SIZE
            )
            logger.info(
                "Página %d/%d | Registros coletados: %d/%d",
                pagina_atual,
                max_paginas if total_registros else "?",
                total_extraido,
                total_registros or "?",
            )

            if len(records) < self.settings.PAGE_SIZE:
                logger.info("Última página atingida. Extração concluída.")
                break

            if total_registros and total_extraido >= total_registros:
                break

            params["pagina"] += 1

        logger.info("Total de registros extraídos: %d", total_extraido)

    def close(self) -> None:
        """Fecha a sessão HTTP e libera recursos de rede."""
        self.session.close()
        logger.debug("Sessão HTTP encerrada.")
