from dataclasses import dataclass
from typing import Optional

import pytest
from bson import ObjectId

from mongoflex.connection import connect
from mongoflex.models import Model, to_collection_name


def test_can_use_model_normally():
    @dataclass
    class Config(Model):
        pass

    assert Config.collection == "configs"

    @dataclass
    class User(Model):
        database = "authentication"

    assert User.database == "authentication"
    assert User.collection == "users"


def test_can_change_database():
    class Config(Model):
        database = "authentication"

    assert Config.database == "authentication"


def test_dataclass_model_should_have_id():
    @dataclass
    class UserDataclassModel(Model):
        pass

    _id = ObjectId("5f9c0b9a9d9b3f1d7f9d6b1a")

    data = UserDataclassModel(_id=_id)

    assert data._id == _id


def test_dataclass_model_fields_and_to_dict():
    @dataclass
    class ModelWithFields(Model):
        text: str
        datetime: Optional[str] = None

    data = ModelWithFields("text")

    assert data.text == "text"
    assert data.datetime is None
    assert data.to_dict() == {"_id": data._id, "text": "text", "datetime": None}


def test_word_inflector():
    assert to_collection_name("Article") == "articles"
    assert to_collection_name("SomeComposedName") == "some_composed_names"


@dataclass
class MyModel(Model):
    text: str


def test_model_update():
    data = MyModel("initial text")

    data.save()

    assert data.text == "initial text"

    data.update(text="Text Updated")

    db_data = MyModel.objects.find_one({"_id": data._id})

    assert data.text == "Text Updated"
    assert db_data["text"] == "Text Updated"

    with pytest.raises(KeyError):
        data.update(unknown_field="Text Updated")


def test_model_save():
    data = MyModel("initial text")

    data.save()

    assert MyModel.objects.find_one({"_id": data._id}) == data.to_dict()


def test_model_find():
    data = MyModel("initial text")

    MyModel.objects.insert_one(data.to_dict())

    res = next(MyModel.find({"_id": data._id}))

    assert res == data
    assert res._id == data._id
    assert res.text == data.text


def test_model_find_return_empty_iterator_when_not_found():
    res = MyModel.find({"_id": ObjectId()})
    assert list(res) == []


def test_model_find_one():
    data = MyModel("initial text")

    MyModel.objects.insert_one(data.to_dict())

    res = MyModel.find_one({"_id": data._id})

    assert res == data
    assert res._id == data._id
    assert res.text == data.text


def test_model_find_one_null_when_not_found():
    res = MyModel.find_one({"_id": ObjectId()})

    assert res is None


def test_can_use_different_databases():
    uri = "mongodb://localhost:27017/sample_database"

    connect(uri, client_name="sample_database")

    @dataclass
    class Sample(Model):
        class Meta:
            client_name = "sample_database"

        text: str

    @dataclass
    class Other(Model):
        text: str

    assert Sample.get_database().name == "sample_database"
    assert Other.get_database().name != "sample_database"


def test_can_define_a_default_meta_for_subclasses():
    uri = "mongodb://localhost:27017/authentication"

    connect(uri, client_name="authentication")

    class Base(Model):
        class Meta:
            client_name = "authentication"

    @dataclass
    class User(Base):
        text: str

    assert User.get_config("client_name") == "authentication"
