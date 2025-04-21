import json
from typing import Optional

from sqlalchemy import create_engine, MetaData, inspect, Table
from sqlalchemy.engine import Engine
from sqlalchemy.exc import NoSuchTableError
from sqlalchemy.orm import sessionmaker
from sqlalchemy.orm import Session


class EngineController:
    """
    Reads config and registers all DB engines.
    Acts as the parent/manager of all engines.
    """

    def __init__(self, config_path: str = "db_config.json"):
        self.config_path = config_path
        self.engines: dict[str, Engine] = {}
        self.session_factories = {}

        self._load_config()
        self._register_engines_sessions()

    def _load_config(self):
        with open(self.config_path) as f:
            self.config = json.load(f)

    def _register_engines_sessions(self):
        for db_name, db_config in self.config.get("dbs", {}).items():
            dialect = db_config["dialect"]
            username = db_config["username"]
            password = db_config["password"]
            host = db_config["host"]
            port = db_config["port"]
            database = db_config["database"]

            url = f"{dialect}://{username}:{password}@{host}:{port}/{database}"

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

    def check_model(self, model_name: str):
        if "." not in model_name:
            raise Exception({"error": "Invalid modelName format.", "code": "GA-005"})

        input_db_name, table_name = model_name.split(".")

        if self.engine is None:
            raise Exception({"error": "DB not found.", "code": "GA-006"})

        try:
            table = Table(table_name, self.meta, autoload_with=self.engine)
        except NoSuchTableError:
            raise Exception(
                {
                    "error": f"{table_name} not found in DB '{input_db_name}'",
                    "code": "GA-007",
                }
            )

        return table

    def find_field(self, field: str):
        try:
            tb_name, engine, fld = None, None, None

            if len(field.split(".")) == 3:
                db_name, tb_name, fld = field.split(".")
                self.check_model(f"{db_name}.{tb_name}")

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
            raise Exception({"error": f"{tb_name} does not exist.", "code": "GA-008"})
        except Exception as e:
            raise Exception({"error": str(e), "code": "GA-009"})


# # todo: create separate class to create engines, only the engine should come here.
# # here only engine should come, the parent class should tell which db this engine is and etc.
# class EngineRegistry:
#
#     def __init__(self, config_path: str = "db_config.json"):
#
#         # Read the config once
#         with open(config_path) as f:
#             self.config = json.load(f)
#
#         self.engine_registry: dict[str, Engine] = {}
#         self._register_engines()
#
#     def _register_engines(self):
#
#         for db_name, dg_config in self.config.get("dbs", {}).items():
#             dialect1 = dg_config["dialect"]
#             username = dg_config["username"]
#             password = dg_config["password"]
#             host = dg_config["host"]
#             port = dg_config["port"]
#             database = dg_config["database"]
#
#             url = f"{dialect1}://{username}:{password}@{host}:{port}/{database}"
#
#             self.engine_registry[db_name] = create_engine(
#                 url, pool_pre_ping=True, pool_recycle=1800
#             )
#
#     def get_engine(self, db_name: str) -> Engine or None:
#         return self.engine_registry.get(db_name)
#
#     def check_model(self, model_name: str):
#
#         if "." not in model_name:
#             raise Exception({"error": "Invalid modelName format.", "code": "GA-005"})
#
#         input_db_name, table_name = model_name.split(".")
#
#         engine = self.get_engine(input_db_name)
#
#         if engine is None:
#             raise Exception({"error": "Invalid db in modelName.", "code": "GA-006"})
#
#         meta = MetaData()
#         try:
#             table = Table(table_name, meta, autoload_with=engine)
#         except NoSuchTableError:
#             raise Exception(
#                 {"error": f"{table} table not found in DB schema", "code": "GA-007"}
#             )
#
#         return input_db_name, table
#
#     def find_field(self, field):
#         try:
#             tb_name, engine, fld = None, None, None
#
#             if len(field.split(".")) == 3:
#                 db_name, tb_name, fld = field.split(".")
#                 self.check_model(f"{db_name}.{tb_name}")
#                 engine = self.get_engine(db_name)
#             elif len(field.split(".")) == 2:
#                 tb_name, fld = field.split(".")
#                 for name, engine_url in self.engine_registry.items():
#                     ins = inspect(engine_url)
#                     if not ins.has_table(tb_name):
#                         continue
#                     if ins.has_table(tb_name):
#                         engine = engine_url
#
#             meta = MetaData()
#             table = Table(tb_name, meta, autoload_with=engine)
#
#             column = getattr(table.c, fld, None)
#             return column
#
#         except NoSuchTableError:
#             raise Exception({"error": f"{tb_name} does not exist.", "code": "GA-008"})
#         except Exception as e:
#             raise Exception({"error": str(e), "code": "GA-009"})
