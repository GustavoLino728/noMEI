"""
Módulo de carga de dados no MongoDB Atlas.

Responsável por conectar ao cluster, criar índices e persistir documentos
transformados utilizando operações de upsert.
"""

import logging
import time
from datetime import datetime, timezone
from typing import Any

from pymongo import MongoClient, UpdateOne
from pymongo.collection import Collection
from pymongo.errors import BulkWriteError, ConnectionFailure

from config.settings import Settings

logger = logging.getLogger(__name__)


class MongoDBLoader:
    """
    Carrega documentos transformados no MongoDB Atlas.

    Utiliza operações em lote (bulk_write) com upsert baseado no campo _id,
    garantindo idempotência e atualização parcial dos dados.
    """

    def __init__(self, settings: Settings) -> None:
        """
        Inicializa o loader.

        Args:
            settings (Settings): Configurações da aplicação.
        """
        self.settings = settings
        self._client: MongoClient | None = None
        self._collection: Collection | None = None

    def connect(self) -> None:
        """
        Conecta ao MongoDB Atlas e prepara a coleção principal.
        """
        self._client = MongoClient(
            self.settings.MONGODB_URI,
            serverSelectionTimeoutMS=10_000,
            connectTimeoutMS=10_000,
            socketTimeoutMS=30_000,
        )

        try:
            self._client.admin.command("ping")
            logger.info("Conexão com MongoDB Atlas estabelecida.")
        except ConnectionFailure:
            logger.error("Falha ao conectar ao MongoDB Atlas.")
            raise

        db = self._client[self.settings.MONGODB_DATABASE]
        self._collection = db[self.settings.MONGODB_COLLECTION]

        self._ensure_indexes()

    def _ensure_indexes(self) -> None:
        """
        Cria índices para melhorar performance de consulta.
        """
        assert self._collection is not None

        self._collection.create_index("modalidadeId", background=True)
        self._collection.create_index("situacaoCompraId", background=True)

        self._collection.create_index(
            [("unidadeOrgao.ufSigla", 1), ("dataAberturaProposta", -1)],
            background=True,
        )

        logger.debug("Índices verificados/criados.")

    def load_batch(self, documents: list[dict[str, Any]]) -> dict[str, int]:
        """
        Carrega documentos na coleção principal.

        Args:
            documents (list): Documentos transformados.

        Returns:
            dict: Estatísticas da operação.
        """
        if self._collection is None:
            raise RuntimeError("Loader não conectado.")

        if not documents:
            return {"inserted": 0, "modified": 0, "errors": 0}

        start_time = time.time()

        valid_docs = [doc for doc in documents if "_id" in doc]

        if len(valid_docs) != len(documents):
            logger.warning(
                "Ignorados %d documentos sem _id",
                len(documents) - len(valid_docs),
            )

        operations = [
            UpdateOne(
                {"_id": doc["_id"]},
                {
                    "$set": doc,
                    "$setOnInsert": {
                        "_created_at": datetime.now(tz=timezone.utc)
                    },
                },
                upsert=True,
            )
            for doc in valid_docs
        ]

        try:
            result = self._collection.bulk_write(operations, ordered=False)

            duration = time.time() - start_time

            logger.info(
                "Batch principal (%d docs) | Inseridos: %d | Atualizados: %d | Tempo: %.2fs",
                len(valid_docs),
                result.upserted_count,
                result.modified_count,
                duration,
            )

            return {
                "inserted": result.upserted_count,
                "modified": result.modified_count,
                "errors": 0,
            }

        except BulkWriteError as exc:
            erros = len(exc.details.get("writeErrors", []))

            logger.error("BulkWriteError no batch principal: %d erro(s)", erros)

            return {
                "inserted": exc.details.get("nUpserted", 0),
                "modified": exc.details.get("nModified", 0),
                "errors": erros,
            }

    def load_into_collection(self, documents: list[dict], collection_name: str) -> None:
        """
        Carrega documentos em uma coleção específica.

        Args:
            documents (list): Documentos a serem inseridos.
            collection_name (str): Nome da coleção destino.
        """
        if self._client is None:
            raise RuntimeError("MongoDB não conectado.")

        if not documents:
            return

        valid_docs = [doc for doc in documents if "_id" in doc]

        if not valid_docs:
            return

        db = self._client[self.settings.MONGODB_DATABASE]
        collection = db[collection_name]

        operations = [
            UpdateOne(
                {"_id": doc["_id"]},
                {
                    "$set": doc,
                    "$setOnInsert": {
                        "_created_at": datetime.now(tz=timezone.utc)
                    },
                },
                upsert=True,
            )
            for doc in valid_docs
        ]

        try:
            result = collection.bulk_write(operations, ordered=False)

            logger.info(
                "Collection '%s' (%d docs) | Inseridos: %d | Atualizados: %d",
                collection_name,
                len(valid_docs),
                result.upserted_count,
                result.modified_count,
            )

        except BulkWriteError as exc:
            logger.error(
                "Erro ao inserir na collection '%s': %s",
                collection_name,
                str(exc),
            )

    def close(self) -> None:
        """
        Fecha a conexão com o MongoDB.
        """
        if self._client:
            self._client.close()
            self._client = None
            self._collection = None
            logger.debug("Conexão MongoDB encerrada.")

    def __enter__(self) -> "MongoDBLoader":
        """
        Context manager — abre conexão automaticamente.
        """
        self.connect()
        return self

    def __exit__(self, *_: Any) -> None:
        """
        Context manager — fecha conexão automaticamente.
        """
        self.close()