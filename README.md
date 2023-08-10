# Mongoflex

Mongoflex is a ORM-like wrapper for mongodb to allow declarative creation and management of mongo collections.

## Installation

```
pip install mongoflex
```

## Usage
First you need to connect to a mongodb database:

```python
from mongoflex import connect

connect("mongodb://localhost:27017/test")
```

And define a model for your collection using python dataclass syntax and extending mongoflex `Model` class.

```python
from dataclasses import dataclass

from mongoflex import Model

@dataclass
class Person(Model):
    age: int
    name: str
```

You can create a record with:

```python
>>> Person(age=10, name="John").save()
>>> list(Person.objects.find())
[{'_id': ObjectId('64d53e4e137297ea7f307086'), 'age': 10, 'name': 'John'}]
```

And you have full access to pymongo `Collection` using:

```python
>>> Person.objects # Collection
Collection(Database(MongoClient(host=['localhost:27017'], document_class=dict, tz_aware=False, connect=True), 'test'), 'people')
```

## Important Points

1. The name of the collection on mongo will be the combination of the words in the name of the `Model` class, with the last pluralized. eg.: `FeedConfig` will become `feed_configs` on mongodb.
2. The `_id` is a default field on every model, but you can define it on constructor.
