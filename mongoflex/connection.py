from typing import Any, Mapping

from pymongo import MongoClient

__all__ = [
    "DEFAULT_URI",
    "NotConnectedError",
    "ConnectionManager",
    "connect",
    "get_database",
]

DEFAULT_URI = "mongodb://localhost:27017/test"
DEFAULT_CLIENT_NAME = "default"


class NotConnectedError(Exception):
    pass


class ConnectionManager:
    clients: Mapping[str, MongoClient] = {}

    @classmethod
    def connect(
        cls, host: str, client_name: str = DEFAULT_CLIENT_NAME, **kwargs: Any
    ) -> MongoClient:
        cls.clients[client_name] = MongoClient(host=host, **kwargs)

        return cls.clients[client_name]

    @classmethod
    def get_client(cls, client_name: str = DEFAULT_CLIENT_NAME) -> MongoClient:
        if not cls.clients.get(client_name):
            raise NotConnectedError(f"No connection named {client_name}")

        return cls.clients[client_name]

    @classmethod
    def get_database(
        cls, db_name: str, client_name: str = DEFAULT_CLIENT_NAME
    ) -> MongoClient:
        client = cls.get_client(client_name=client_name)

        return client.get_database(db_name)


def connect(
    host: str = DEFAULT_URI, client_name: str = DEFAULT_CLIENT_NAME, **kwargs: Any
) -> MongoClient:
    return ConnectionManager.connect(host, client_name=client_name, **kwargs)


def get_database(db_name: str, client_name: str = DEFAULT_CLIENT_NAME) -> MongoClient:
    return ConnectionManager.get_database(db_name, client_name=client_name)
