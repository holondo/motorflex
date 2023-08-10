from mongoflex.connection import connect
from mongoflex.models import ModelMeta


def pytest_configure(config):
    connect()


def pytest_unconfigure(config):
    for model in ModelMeta.models:
        model.objects.drop()
