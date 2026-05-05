"""
Ponto de entrada da aplicação ETL do PNCP.

Responsável por:
- Processar argumentos de linha de comando
- Validar entradas
- Executar o pipeline ETL
"""

import argparse
import logging
import sys
from datetime import datetime

from config.settings import Settings
from src.pipeline import ETLPipeline


def _configure_logging() -> None:
    """
    Configura o sistema de logging.
    """
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [%(levelname)s] %(name)s — %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
        handlers=[logging.StreamHandler(sys.stdout)],
    )


def validar_data(data: str) -> str:
    """
    Valida formato de data YYYYMMDD.

    Args:
        data (str): Data informada.

    Returns:
        str: Data válida.

    Raises:
        argparse.ArgumentTypeError: Se formato inválido.
    """
    try:
        datetime.strptime(data, "%Y%m%d")
        return data
    except ValueError:
        raise argparse.ArgumentTypeError(
            "Formato inválido. Use YYYYMMDD."
        )


def validar_intervalo(data_inicial: str | None, data_final: str) -> None:
    """
    Valida consistência entre datas.

    Args:
        data_inicial (str | None): Data inicial.
        data_final (str): Data final.

    Raises:
        ValueError: Se intervalo inválido.
    """
    df = datetime.strptime(data_final, "%Y%m%d")

    if df > datetime.now():
        raise ValueError("data_final não pode estar no futuro")

    if data_inicial:
        di = datetime.strptime(data_inicial, "%Y%m%d")

        if di > df:
            raise ValueError("data_inicial não pode ser maior que data_final")


def _parse_args() -> argparse.Namespace:
    """
    Analisa argumentos da linha de comando.

    Returns:
        argparse.Namespace: Argumentos parseados.
    """
    parser = argparse.ArgumentParser(
        description="ETL PNCP → MongoDB Atlas"
    )

    parser.add_argument(
        "--data-final",
        type=validar_data,
        default=datetime.now().strftime("%Y%m%d"),
        help="Data final (YYYYMMDD)",
    )

    parser.add_argument(
        "--data-inicial",
        type=validar_data,
        help="Data inicial (YYYYMMDD)",
    )

    parser.add_argument(
        "--uf",
        help="UF (ex: pe, sp, rj)",
    )

    parser.add_argument(
        "--modalidade",
        type=int,
        help="Código da modalidade",
    )

    parser.add_argument(
        "--cnpj",
        help="CNPJ do órgão",
    )

    parser.add_argument(
        "--municipio-ibge",
        help="Código IBGE",
    )
    
    parser.add_argument(
        "--max-paginas",
        type=int,
        default=None,
        help="Limite de páginas para testes",
    )  

    return parser.parse_args()


def main() -> int:
    """
    Função principal da aplicação.

    Returns:
        int: Código de saída (0 sucesso, 1 erro).
    """
    _configure_logging()
    logger = logging.getLogger(__name__)

    args = _parse_args()

    try:
        validar_intervalo(args.data_inicial, args.data_final)
    except ValueError as e:
        logger.error("Erro de validação: %s", str(e))
        return 1

    settings = Settings()

    try:
        settings.validate()
    except ValueError as exc:
        logger.error("Configuração inválida: %s", exc)
        return 1

    extract_params = {
        "data_final": args.data_final,
        "data_inicial": args.data_inicial,
        "uf": args.uf,
        "codigo_modalidade": args.modalidade,
        "cnpj": args.cnpj,
        "codigo_municipio_ibge": args.municipio_ibge,
        "max_paginas": args.max_paginas,
    }
    
    extract_params = {k: v for k, v in extract_params.items() if v is not None}

    logger.info("Parâmetros finais: %s", extract_params)

    pipeline = ETLPipeline(settings)
    result = pipeline.run(extract_params)

    return 0 if result.sucesso else 1


if __name__ == "__main__":
    sys.exit(main())