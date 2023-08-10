from typing import Any

from pymongo import MongoClient

__all__ = [
    "DEFAULT_URI",
    "NotConnectedError",
    "ConnectionManager",
    "connect",
    "get_database",
]

DEFAULT_URI = "mongodb://localhost:27017/test"


class NotConnectedError(Exception):
    pass


class ConnectionManager:
    client: MongoClient = None

    @classmethod
    def connect(cls, host: str, **kwargs: Any) -> MongoClient:
        cls.client = MongoClient(host=host, **kwargs)

        return cls.client

    @classmethod
    def get_client(cls) -> MongoClient:
        if not cls.client:
            raise NotConnectedError("Not connected to MongoDB")

        return cls.client

    @classmethod
    def get_database(cls, db_name: str) -> MongoClient:
        client = cls.get_client()

        return client.get_database(db_name)


def connect(host: str = DEFAULT_URI, **kwargs: Any) -> MongoClient:
    return ConnectionManager.connect(host, **kwargs)


def get_database(db_name: str) -> MongoClient:
    return ConnectionManager.get_database(db_name)
