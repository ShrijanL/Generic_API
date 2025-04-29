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

    def __init__(self, db_name: str = None, config_path: str = None):
        if not config_path:
            config_path = os.path.join(os.path.dirname(__file__), "db_config.json")

        self.config_path = config_path
        self.engine: Optional[Engine] = None
        self.session_factory: Optional[sessionmaker] = None
        self.active_db_name: Optional[str] = None

        self._load_config()
        self._register_engine_session(db_name)

    def _create_all_tables(self, metadata: MetaData):
        for engine in self.engine.values():
            metadata.create_all(bind=engine)

    def _load_config(self):
        with open(self.config_path) as f:
            self.config = json.load(f)

    def _register_engine_session(self, db_name: str):
        db_config = self.config.get("dbs", {}).get(db_name)
        if not db_config:
            raise_exception(error=f"DB {db_name} not found in config.", code="GA-008")

        dialect = db_config.get("dialect")
        username = db_config.get("username")
        password = db_config.get("password")
        host = db_config.get("host")
        port = db_config.get("port")
        database = db_config.get("database")

        url = f"{dialect}://{username}:{password}@{host}:{port}/{database}"
        if database == "test_db":
            url = f"{dialect}:///{database}"  # SQLite in-memory DB for test

        self.engine = create_engine(url, pool_pre_ping=True, pool_recycle=1800)
        self.session_factory = sessionmaker(bind=self.engine)
        self.active_db_name = db_name

    def retrieve_engine(self) -> Engine:
        return self.engine

    def retrieve_session(self) -> Session:
        return self.session_factory()


class EngineRegistry:
    """
    Acts as a consumer/child of EngineController.
    Focused on one DB engine at a time.
    """

    # def __init__(self, db_name: str, config_path:str = None):
    def __init__(self, db_name: str, config_path: str):
        self.controller = EngineController(db_name, config_path=config_path)
        self.engine = self.controller.retrieve_engine()
        self.session = self.controller.retrieve_session()
        self.meta = MetaData()

    def get_session(self) -> Optional[Session]:
        return self.session

    def check_model(self, input_db_name: str, table_name: str):

        if self.engine is None:
            raise_exception(error="DB not found.", code="GA-009")

        try:
            table = Table(table_name, self.meta, autoload_with=self.engine)
        except NoSuchTableError:
            raise_exception(
                error=f"{table_name} not found in DB {input_db_name}", code="GA-010"
            )

        return table

    def find_field(self, field: str):
        tb_name, engine, fld = None, None, None
        try:

            if len(field.split(".")) == 3:
                db_name, tb_name, fld = field.split(".")
                self.check_model(db_name, tb_name)

            elif len(field.split(".")) == 2:
                tb_name, fld = field.split(".")
                ins = inspect(self.engine)
                if not ins.has_table(tb_name):
                    raise_exception(error=f"{tb_name} not found in DB", code="GA-011")

            table = Table(tb_name, self.meta, autoload_with=self.engine)

            column = getattr(table.c, fld, None)
            return column

        except NoSuchTableError:
            raise_exception(error=f"{tb_name} does not exist.", code="GA-012")
        except Exception as e:
            raise_exception(error=str(e), code="GA-013")
