"""
Ponto de entrada da aplicação ETL do PNCP.

Exemplo de uso:
    python main.py
    python main.py --uf pe --modalidade 8
    python main.py --data-inicial 20260401 --data-final 20260409 --uf sp
"""

import argparse
import logging
import sys
from datetime import datetime

from config.settings import Settings
from src.pipeline import ETLPipeline


def _configure_logging() -> None:
    """Configura o sistema de logging com formato padronizado."""
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [%(levelname)s] %(name)s — %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
        handlers=[
            logging.StreamHandler(sys.stdout),
        ],
    )


def validar_data(data: str) -> str:
    """Valida que a data está no formato YYYYMMDD."""
    try:
        datetime.strptime(data, "%Y%m%d")
        return data
    except ValueError:
        raise argparse.ArgumentTypeError(
            "Formato inválido. Use YYYYMMDD."
        )


def _parse_args() -> argparse.Namespace:
    """
    Analisa os argumentos de linha de comando.

    Returns:
        argparse.Namespace: Namespace com os argumentos parseados.
    """
    parser = argparse.ArgumentParser(
        description="ETL — Coleta de Contratações/Propostas do PNCP para MongoDB Atlas"
    )

    parser.add_argument(
        "--data-final",
        type=validar_data,
        default="20260430",
        metavar="YYYYMMDD",
        help="Data final de encerramento de propostas (padrão: 20260430)",
    )

    parser.add_argument(
        "--data-inicial",
        type=validar_data,
        default=None,
        metavar="YYYYMMDD",
        help="Data inicial de encerramento de propostas (opcional)",
    )

    parser.add_argument(
        "--uf",
        default=None,
        help="Sigla da UF para filtrar (ex: pe, sp, rj)",
    )

    parser.add_argument(
        "--modalidade",
        type=int,
        default=None,
        metavar="CODIGO",
        help="Código da modalidade de contratação (ex: 8)",
    )

    parser.add_argument(
        "--cnpj",
        default=None,
        help="CNPJ do órgão contratante (somente números)",
    )

    parser.add_argument(
        "--municipio-ibge",
        default=None,
        metavar="CODIGO",
        help="Código IBGE do município",
    )

    return parser.parse_args()


def main() -> int:
    """
    Função principal — configura e executa o pipeline ETL.

    Returns:
        int: Código de saída (0 = sucesso, 1 = falha).
    """
    _configure_logging()
    logger = logging.getLogger(__name__)

    args = _parse_args()

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
    }
    # Remove parâmetros não fornecidos
    extract_params = {k: v for k, v in extract_params.items() if v is not None}

    pipeline = ETLPipeline(settings)
    result = pipeline.run(extract_params)

    return 0 if result.sucesso else 1


if __name__ == "__main__":
    sys.exit(main())
