import os

import pytest
from fastapi.testclient import TestClient

from api.db_conn import EngineController
from tests import models
import shutil

Base = models.Base


@pytest.fixture(scope="session")
def engine_controller():
    """
    Initializes a shared `EngineController` instance for the test session and manages schema lifecycle.
    Creates and drops schema for test db.
    """
    config_path = os.path.join(os.path.dirname(__file__), "test_db_config.json")
    controller = EngineController(config_path=config_path)

    # create tables in test db
    Base.metadata.create_all(bind=controller.engines["test_db"])

    yield controller

    # delete tables in test db
    Base.metadata.drop_all(bind=controller.engines["test_db"])


@pytest.fixture(scope="function")
def db_session(engine_controller):
    """
    Provides a clean DB session for each test by truncating all tables.
    """
    session = engine_controller.retrieve_session("test_db")

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
    mock_controller = mocker.patch("api.db_conn.EngineController")
    config_path = os.path.join(os.path.dirname(__file__), "test_db_config.json")

    mock_controller.return_value = EngineController(config_path=config_path)

    from api.main import app  # Import after patching

    # Return the TestClient
    return TestClient(app)
