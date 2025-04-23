import json
import os.path
from typing import Optional

from sqlalchemy import create_engine, MetaData, inspect, Table
from sqlalchemy.engine import Engine
from sqlalchemy.exc import NoSuchTableError
from sqlalchemy.orm import Session
from sqlalchemy.orm import sessionmaker

from .utils import raise_exception


class EngineController:
    """
    Reads config and registers all DB engines.
    Acts as the parent/manager of all engines.
    """

    def __init__(self, config_path: str = None):

        if not config_path:
            config_path = os.path.join(os.path.dirname(__file__), "db_config.json")

        self.config_path = config_path
        self.engines: dict[str, Engine] = {}
        self.session_factories = {}

        self._load_config()
        self._register_engines_sessions()

    def _create_all_tables(self, metadata: MetaData):
        for engine in self.engines.values():
            metadata.create_all(bind=engine)

    def _load_config(self):
        with open(self.config_path) as f:
            self.config = json.load(f)

    def _register_engines_sessions(self):
        for db_name, db_config in self.config.get("dbs", {}).items():
            dialect = db_config.get("dialect")
            username = db_config.get("username")
            password = db_config.get("password")
            host = db_config.get("host")
            port = db_config.get("port")
            database = db_config.get("database")

            url = f"{dialect}://{username}:{password}@{host}:{port}/{database}"
            if database == "test_db":
                url = f"{dialect}:///{database}"  # SQLite in-memory DB for test

            engine = create_engine(url, pool_pre_ping=True, pool_recycle=1800)
            self.engines[db_name] = engine

            SessionFactory = sessionmaker(bind=engine)
            self.session_factories[db_name] = SessionFactory

    def retrieve_engine(self, db_name: str) -> Optional[Engine]:
        eng = self.engines.get(db_name)
        return eng

    def retrieve_session(self, db_name: str) -> Optional[Session]:
        SessionFactory = self.session_factories.get(db_name)
        return SessionFactory() if SessionFactory else None


class EngineRegistry:
    """
    Acts as a consumer/child of EngineController.
    Focused on one DB engine at a time.
    """

    def __init__(self, db_name: str, controller: EngineController):
        self.controller = controller
        self.db_name = db_name
        self.session = self.controller.retrieve_session(db_name)
        self.engine = self.controller.retrieve_engine(db_name)

        self.meta = MetaData()

    def get_session(self) -> Optional[Session]:
        return self.session

    def check_model(self, input_db_name: str, table_name: str):

        if self.engine is None:
            raise_exception(error="DB not found.", code="GA-006")

        try:
            table = Table(table_name, self.meta, autoload_with=self.engine)
        except NoSuchTableError:
            raise_exception(
                error=f"{table_name} not found in DB {input_db_name}", code="GA-007"
            )

        return table

    def find_field(self, field: str):
        try:
            tb_name, engine, fld = None, None, None

            if len(field.split(".")) == 3:
                db_name, tb_name, fld = field.split(".")
                self.check_model(db_name, tb_name)

            elif len(field.split(".")) == 2:
                tb_name, fld = field.split(".")
                for name, engine_url in self.controller.engines.items():
                    ins = inspect(engine_url)
                    if not ins.has_table(tb_name):
                        continue
                    if ins.has_table(tb_name):
                        self.engine = engine_url

            table = Table(tb_name, self.meta, autoload_with=self.engine)

            column = getattr(table.c, fld, None)
            return column

        except NoSuchTableError:
            raise_exception(error=f"{tb_name} does not exist.", code="GA-008")
        except Exception as e:
            raise_exception(error=str(e), code="GA-009")
