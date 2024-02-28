import re
from dataclasses import asdict, dataclass, field, fields
from typing import Any, Dict, Generic, Iterable, Mapping, Optional, TypeVar

from bson import ObjectId
from inflect import engine
from motor.motor_asyncio import (
    AsyncIOMotorClient,
    AsyncIOMotorCollection,
    AsyncIOMotorDatabase,
)
from pymongo import IndexModel

__all__ = ["Model", "ModelMeta"]

inflector = engine()

T = TypeVar("T", bound="Model")


class MetaConfig:
    client_name: str
    database_name: str


def to_collection_name(class_name: str) -> str:
    words = [x.lower() for x in re.findall("[A-Z][^A-Z]*", class_name)]
    words[-1] = inflector.plural(words[-1])
    return "_".join(words)


class ModelMeta(type):
    models = []

    def __new__(cls, name, bases, attrs):
        if name not in ["Model", "BaseModel"]:
            attrs["collection_name"] = to_collection_name(name)
            attrs["_id"] = field(default_factory=ObjectId)

            if not attrs.get("__annotations__"):
                attrs["__annotations__"] = {}

            attrs["__annotations__"]["_id"] = ObjectId

        model = super().__new__(cls, name, bases, attrs)

        if model.__name__ not in ["Model", "BaseModel"]:
            cls.models.append(model)

        return model

    def __init_subclass__(
        cls, /, database: str = None, collection: str = None, **kwargs
    ) -> None:
        super().__init_subclass__(**kwargs)

        cls.database_name = cls.database_name or database
        cls.collection_name = cls.collection_name or collection

    @classmethod
    def get_client(cls) -> AsyncIOMotorClient:
        client_name = cls.get_config("client_name", "default")
        return AsyncIOMotorClient(client_name)

    @classmethod
    def get_database(cls) -> AsyncIOMotorDatabase:
        client = cls.get_client()
        db_name = cls.get_config("database_name")
        return client[db_name]

    @classmethod
    def get_collection(cls) -> AsyncIOMotorCollection:
        db = cls.get_database()
        return db[cls.collection_name]

    @classmethod
    def get_config(cls, name: str, default: Any = None):
        config = getattr(cls, "Meta", {})
        return getattr(config, name, default)


class BaseModel(metaclass=ModelMeta):
    INDEXES: Iterable[IndexModel] = []

    @classmethod
    async def create_indexes(cls):
        collection = cls.get_collection()
        if cls.INDEXES:
            await collection.create_indexes(cls.INDEXES)


def as_model(func):
    async def wrapper(cls, *args, **kwargs):
        response = await func(cls, *args, **kwargs)

        if not response:
            return response

        if isinstance(response, dict):
            return cls.from_dict(response)

        return map(cls.from_dict, await func(cls, *args, **kwargs))

    return wrapper


@dataclass
class Model(BaseModel, Generic[T]):
    def to_dict(self):
        return asdict(self)

    @classmethod
    def from_dict(cls, document: Dict[str, Any]) -> T:
        allowed_fields = [x.name for x in fields(cls)]
        return cls(**{k: v for k, v in document.items() if k in allowed_fields})

    async def update(self, **kwargs):
        allowed_fields = [x.name for x in fields(self)]

        for key in kwargs.keys():
            if key not in allowed_fields:
                raise KeyError(f"Key {key} not allowed")

        await self.__class__.get_collection().update_one(
            {"_id": self._id}, {"$set": kwargs}
        )

        for key, value in kwargs.items():
            setattr(self, key, value)

    async def save(self):
        await self.__class__.get_collection().update_one(
            {"_id": self._id}, {"$set": self.to_dict()}, upsert=True
        )

    @classmethod
    @as_model
    async def find_one(
        cls, filter: Optional[Any] = None, *args: Any, **kwargs: Any
    ) -> Optional[T]:
        return await cls.get_collection().find_one(filter, *args, **kwargs)

    @classmethod
    @as_model
    async def find(
        cls, filter: Mapping[str, Any] = None, *args: Any, **kwargs: Any
    ) -> Iterable[T]:
        return cls.get_collection().find(filter, *args, **kwargs)
