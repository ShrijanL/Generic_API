import os

import pytest
from fastapi.testclient import TestClient

from api.db_conn import EngineRegistry
from tests import models

Base = models.Base

db_path = os.path.abspath(os.path.join(os.path.dirname(__file__), "db_config.json"))


@pytest.fixture(scope="session")
def engine_controller():
    """
    Initializes a shared `EngineController` instance for the test session and manages schema lifecycle.
    Creates and drops schema for test db.
    """
    controller = EngineRegistry("test_db", config_path=db_path)

    # create tables in test db
    Base.metadata.create_all(bind=controller.engine)

    yield controller

    # delete tables in test db
    Base.metadata.drop_all(bind=controller.engine)


@pytest.fixture(scope="function")
def db_session(engine_controller):
    """
    Provides a clean DB session for each test by truncating all tables.
    """
    session = engine_controller.session

    # Clear all data before each test
    for table in reversed(Base.metadata.sorted_tables):
        session.execute(table.delete())
    session.commit()

    yield session

    # Optionally clear again after test (defensive)
    for table in reversed(Base.metadata.sorted_tables):
        session.execute(table.delete())
    session.commit()

    session.close()


@pytest.fixture(scope="function")
def client(mocker):
    # Mock EngineController in your test setup

    from api.main import app  # Import after patching

    # Return the TestClient
    return TestClient(app)
