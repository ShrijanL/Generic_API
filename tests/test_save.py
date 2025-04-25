from datetime import date

import pytest
from sqlalchemy import text, bindparam

from fixtures import engine_controller, db_session, client
from instances import class1, class2, class3, customer3, customer1
from utils import create_test_records, get_access_token


class TestGenericSave:

    @pytest.fixture(autouse=True)
    def _setup(self, engine_controller):
        """
        Automatically sets up engine controller for each test method.
        """
        self.engine_controller = engine_controller

    def test_save_success(self, client):
        """
        Saves 2 records successfully without fk field
        :param client:
        :return:
        """
        token = get_access_token()
        headers = {"Authorization": f"Bearer {token}"}

        payload = {
            "payload": {
                "modelName": "test_db.customers",
                "id": None,
                "saveInput": [
                    {
                        "name": "test1",
                        "dob": date(2004, 6, 12).isoformat(),
                        "email": "test1@mail.com",
                        "phone_no": "741852",
                        "address": "Hyd",
                        "zip_code": "555",
                        "status": "alive",
                    },
                    {
                        "name": "test2",
                        "dob": date(2004, 5, 22).isoformat(),
                        "email": "test1@mail.com",
                        "phone_no": "852147",
                        "address": "Bombay",
                        "zip_code": "777",
                        "status": "alive",
                    },
                ],
            }
        }

        response = client.post("/save", json=payload, headers=headers)
        res = response.json()

        assert response.status_code == 200
        assert res["message"] == "saved successfully"
        assert res["data"] == [1, 2]

    def test_save_success_with_fk(self, client, db_session):
        """
        Saves 2 records successfully with fk field
        :param client:
        :param db_session:
        :return:
        """
        token = get_access_token()
        headers = {"Authorization": f"Bearer {token}"}

        create_test_records(db_session, class1, class2)

        payload = {
            "payload": {
                "modelName": "test_db.customers",
                "id": None,
                "saveInput": [
                    {
                        "name": "test1_fk",
                        "dob": date(2004, 6, 12).isoformat(),
                        "email": "test1@mail.com",
                        "phone_no": "741852",
                        "address": "Hyd",
                        "zip_code": "555",
                        "status": "alive",
                        "class_student": 1,
                    },
                    {
                        "name": "test2_fk",
                        "dob": date(2004, 5, 22).isoformat(),
                        "email": "test1@mail.com",
                        "phone_no": "852147",
                        "address": "Bombay",
                        "zip_code": "777",
                        "status": "alive",
                        "class_student": 2,
                    },
                ],
            }
        }

        response = client.post("/save", json=payload, headers=headers)
        res = response.json()

        assert response.status_code == 200
        assert res["message"] == "saved successfully"
        assert res["data"] == [1, 2]

        # Check if fk values are correctly inserted or not
        stmt = text(
            "SELECT class_student FROM customers WHERE name IN :names"
        ).bindparams(bindparam("names", expanding=True))

        recs = db_session.execute(stmt, {"names": ["test1_fk", "test2_fk"]}).fetchall()
        assert recs == [(1,), (2,)]

    def test_save_without_db_name(self, client):
        """
        No DB name in payload
        """
        token = get_access_token()
        headers = {"Authorization": f"Bearer {token}"}
        payload = {
            "payload": {
                "modelName": "customers",
                "id": None,
                "saveInput": [
                    {
                        "name": "test1",
                        "dob": date(2004, 6, 12).isoformat(),
                        "email": "test1@mail.com",
                        "phone_no": "741852",
                        "address": "Hyd",
                        "zip_code": "555",
                        "status": "alive",
                    },
                    {
                        "name": "test2",
                        "dob": date(2004, 5, 22).isoformat(),
                        "email": "test1@mail.com",
                        "phone_no": "852147",
                        "address": "Bombay",
                        "zip_code": "777",
                        "status": "alive",
                    },
                ],
            }
        }

        response = client.post("/save", json=payload, headers=headers)
        res = response.json()

        assert response.status_code == 400
        assert (
            res["error"]
            == "Value error, modelName must be in format as 'db.table'.('payload', 'modelName')"
        )
        assert res["code"] == "GA-003"

    def test_update_record_success(self, client, db_session):
        """
        Update an existing rec
        :param client:
        :param db_session:
        :return:
        """
        token = get_access_token()
        headers = {"Authorization": f"Bearer {token}"}
        create_test_records(db_session, class3, customer3)

        assert customer3.id == 3
        assert customer3.name == "Name3"

        payload = {
            "payload": {
                "modelName": "test_db.customers",
                "id": customer3.id,
                "saveInput": [
                    {
                        "name": "Name3_updated",
                        "dob": date(2004, 6, 12).isoformat(),
                        "email": "test1@mail.com",
                        "phone_no": "741852",
                        "address": "Hyd",
                        "zip_code": "555",
                        "status": "alive",
                        "class_student": 1,
                    }
                ],
            }
        }

        response = client.post("/save", json=payload, headers=headers)
        res = response.json()

        assert res["message"] == "updated successfully"
        assert res["data"] == [3]
        assert response.status_code == 200

        # Check if fk values are correctly inserted or not
        stmt = text("SELECT name FROM customers WHERE id = :id").bindparams(
            bindparam("id")
        )
        recs = db_session.execute(stmt, {"id": customer3.id}).fetchall()

        assert recs == [("Name3_updated",)]

    def test_create_record_with_incorrect_db(self, client):
        """
        User has sent correct payload format.
        """
        token = get_access_token()
        headers = {"Authorization": f"Bearer {token}"}
        payload = {
            "payload": {
                "modelName": "test_db1.customers",
                "id": None,
                "saveInput": [
                    {
                        "name": "test1",
                        "dob": date(2004, 6, 12).isoformat(),
                        "email": "test1@mail.com",
                        "phone_no": "741852",
                        "address": "Hyd",
                        "zip_code": "555",
                        "status": "alive",
                    }
                ],
            }
        }

        response = client.post("/save", json=payload, headers=headers)
        res = response.json()

        assert response.status_code == 400
        assert res["error"] == "DB not found."
        assert res["code"] == "GA-006"

    def test_create_record_with_incorrect_table(self, client):
        """
        User has sent correct payload format.
        """
        token = get_access_token()
        headers = {"Authorization": f"Bearer {token}"}
        payload = {
            "payload": {
                "modelName": "test_db.customers1",
                "id": None,
                "saveInput": [
                    {
                        "name": "test1",
                        "dob": date(2004, 6, 12).isoformat(),
                        "email": "test1@mail.com",
                        "phone_no": "741852",
                        "address": "Hyd",
                        "zip_code": "555",
                        "status": "alive",
                    }
                ],
            }
        }

        response = client.post("/save", json=payload, headers=headers)
        res = response.json()

        assert response.status_code == 400
        assert res["error"] == "customers1 not found in DB test_db"
        assert res["code"] == "GA-007"

    def test_invalid_payload_format(self, client):
        """
        User has sent incorrect payload format.
        """
        token = get_access_token()
        headers = {"Authorization": f"Bearer {token}"}
        payload = {
            "payload": {
                "id": None,
                "saveInput": [
                    {
                        "name": "test1",
                        "dob": date(2004, 6, 12).isoformat(),
                        "email": "test1@mail.com",
                        "phone_no": "741852",
                        "address": "Hyd",
                        "zip_code": "555",
                        "status": "alive",
                    }
                ],
            }
        }

        response = client.post("/save", json=payload, headers=headers)
        res = response.json()

        assert response.status_code == 400
        assert res["error"] == "Field required('payload', 'modelName')"
        assert res["code"] == "GA-003"

    def test_extra_field_in_payload(self, client):
        """
        User has sent extra field in payload.
        """
        token = get_access_token()
        headers = {"Authorization": f"Bearer {token}"}
        payload = {
            "payload": {
                "modelName": "test_db.customers",
                "id": None,
                "dummy": "hello",
                "saveInput": [
                    {
                        "name": "test1",
                        "dob": date(2004, 6, 12).isoformat(),
                        "email": "test1@mail.com",
                        "phone_no": "741852",
                        "address": "Hyd",
                        "zip_code": "555",
                        "status": "alive",
                    }
                ],
            }
        }

        response = client.post("/save", json=payload, headers=headers)
        res = response.json()

        assert response.status_code == 400
        assert res["error"] == "Extra inputs are not permitted('payload', 'dummy')"
        assert res["code"] == "GA-003"

    def test_model_name_not_str(self, client):
        """
        User has sent incorrect modelName type.
        """
        token = get_access_token()
        headers = {"Authorization": f"Bearer {token}"}
        payload = {
            "payload": {
                "modelName": 123,
                "id": None,
                "saveInput": [
                    {
                        "name": "test1",
                        "dob": date(2004, 6, 12).isoformat(),
                        "email": "test1@mail.com",
                        "phone_no": "741852",
                        "address": "Hyd",
                        "zip_code": "555",
                        "status": "alive",
                    }
                ],
            }
        }

        response = client.post("/save", json=payload, headers=headers)
        res = response.json()

        assert response.status_code == 400
        assert res["error"] == "Input should be a valid string('payload', 'modelName')"
        assert res["code"] == "GA-003"

    def test_recs_more_than_limit(self, client):
        """
        Payload consists more than `CREATE_BATCH_SIZE` recs
        """
        token = get_access_token()
        headers = {"Authorization": f"Bearer {token}"}
        payload = {
            "payload": {
                "modelName": "test_db.customers",
                "id": None,
                "saveInput": [
                    {
                        "name": "test1",
                        "dob": date(2004, 6, 12).isoformat(),
                        "email": "test1@mail.com",
                        "phone_no": "741852",
                        "address": "Hyd",
                        "zip_code": "555",
                        "status": "alive",
                    },
                    {
                        "name": "test2",
                        "dob": date(2004, 5, 22).isoformat(),
                        "email": "test1@mail.com",
                        "phone_no": "852147",
                        "address": "Bombay",
                        "zip_code": "777",
                        "status": "alive",
                    },
                    {
                        "name": "test2",
                        "dob": date(2004, 5, 22).isoformat(),
                        "email": "test1@mail.com",
                        "phone_no": "852147",
                        "address": "Bombay",
                        "zip_code": "777",
                        "status": "alive",
                    },
                    {
                        "name": "test2",
                        "dob": date(2004, 5, 22).isoformat(),
                        "email": "test1@mail.com",
                        "phone_no": "852147",
                        "address": "Bombay",
                        "zip_code": "777",
                        "status": "alive",
                    },
                    {
                        "name": "test2",
                        "dob": date(2004, 5, 22).isoformat(),
                        "email": "test1@mail.com",
                        "phone_no": "852147",
                        "address": "Bombay",
                        "zip_code": "777",
                        "status": "alive",
                    },
                    {
                        "name": "test2",
                        "dob": date(2004, 5, 22).isoformat(),
                        "email": "test1@mail.com",
                        "phone_no": "852147",
                        "address": "Bombay",
                        "zip_code": "777",
                        "status": "alive",
                    },
                    {
                        "name": "test2",
                        "dob": date(2004, 5, 22).isoformat(),
                        "email": "test1@mail.com",
                        "phone_no": "852147",
                        "address": "Bombay",
                        "zip_code": "777",
                        "status": "alive",
                    },
                    {
                        "name": "test2",
                        "dob": date(2004, 5, 22).isoformat(),
                        "email": "test1@mail.com",
                        "phone_no": "852147",
                        "address": "Bombay",
                        "zip_code": "777",
                        "status": "alive",
                    },
                    {
                        "name": "test2",
                        "dob": date(2004, 5, 22).isoformat(),
                        "email": "test1@mail.com",
                        "phone_no": "852147",
                        "address": "Bombay",
                        "zip_code": "777",
                        "status": "alive",
                    },
                    {
                        "name": "test2",
                        "dob": date(2004, 5, 22).isoformat(),
                        "email": "test1@mail.com",
                        "phone_no": "852147",
                        "address": "Bombay",
                        "zip_code": "777",
                        "status": "alive",
                    },
                    {
                        "name": "test2",
                        "dob": date(2004, 5, 22).isoformat(),
                        "email": "test1@mail.com",
                        "phone_no": "852147",
                        "address": "Bombay",
                        "zip_code": "777",
                        "status": "alive",
                    },
                ],
            }
        }

        response = client.post("/save", json=payload, headers=headers)
        res = response.json()

        assert response.status_code == 400
        assert res["error"] == "Only 10 records allowed at a time."
        assert res["code"] == "GA-004"

    def test_update_multiple_records(self, client, db_session):
        """
        update_multiple_records
        """
        token = get_access_token()
        headers = {"Authorization": f"Bearer {token}"}
        create_test_records(db_session, class1, customer1)

        payload = {
            "payload": {
                "modelName": "test_db.customers",
                "id": 1,
                "saveInput": [
                    {
                        "name": "Name3_updated",
                        "dob": date(2004, 6, 12).isoformat(),
                        "email": "test1@mail.com",
                        "phone_no": "741852",
                        "address": "Hyd",
                        "zip_code": "555",
                        "status": "alive",
                        "class_student": 1,
                    },
                    {
                        "name": "Name3_updated",
                        "dob": date(2004, 6, 12).isoformat(),
                        "email": "test1@mail.com",
                        "phone_no": "741852",
                        "address": "Hyd",
                        "zip_code": "555",
                        "status": "alive",
                        "class_student": 1,
                    },
                ],
            }
        }

        response = client.post("/save", json=payload, headers=headers)
        res = response.json()

        assert res["error"] == "Only 1 record to update at once"
        assert res["code"] == "GA-013"
        assert response.status_code == 400

    def test_invalid_field_in_save_input(self, client):
        """
        invalid field in save input
        :param client:
        :return:
        """
        token = get_access_token()
        headers = {"Authorization": f"Bearer {token}"}
        payload = {
            "payload": {
                "modelName": "test_db.customers",
                "id": None,
                "saveInput": [
                    {
                        "name": "test1",
                        "dob": date(2004, 6, 12).isoformat(),
                        "email": "test1@mail.com",
                        "phone_no": "741852",
                        "address": "Hyd",
                        "zip_code": "555",
                        "status": "alive",
                        "ABCD": "dummy",
                    }
                ],
            }
        }

        response = client.post("/save", json=payload, headers=headers)
        res = response.json()

        assert response.status_code == 400
        assert res["error"] == "Extra inputs are not permitted. ('ABCD',)"
        assert res["code"] == "GA-014"

    def test_invalid_data_type(self, client):
        """
        invalid data type in save input
        :param client:
        :return:
        """
        token = get_access_token()
        headers = {"Authorization": f"Bearer {token}"}
        payload = {
            "payload": {
                "modelName": "test_db.customers",
                "id": None,
                "saveInput": [
                    {
                        "name": "test1",
                        "dob": date(2004, 6, 12).isoformat(),
                        "email": "test1@mail.com",
                        "phone_no": "741852",
                        "address": "Hyd",
                        "zip_code": "555",
                        "status": "alive",
                        "is_active": "hello",
                    }
                ],
            }
        }

        response = client.post("/save", json=payload, headers=headers)
        res = response.json()

        assert response.status_code == 400
        assert (
            res["error"]
            == "Input should be a valid boolean, unable to interpret input. ('is_active',)"
        )
        assert res["code"] == "GA-014"

    def test_missing_required_field(self, client):
        """
        Save input does not have `nullable=False` field
        :param client:
        :return:
        """
        token = get_access_token()
        headers = {"Authorization": f"Bearer {token}"}
        payload = {
            "payload": {
                "modelName": "test_db.customers",
                "id": None,
                "saveInput": [
                    {
                        "name": "test1",
                        "dob": date(2004, 6, 12).isoformat(),
                        # "email": "test1@mail.com",
                        "phone_no": "741852",
                        "address": "Hyd",
                        "zip_code": "555",
                        "status": "alive",
                        "is_active": True,
                    }
                ],
            }
        }

        response = client.post("/save", json=payload, headers=headers)
        res = response.json()

        assert response.status_code == 400
        assert res["error"] == "Field required. ('email',)"
        assert res["code"] == "GA-014"

    def test_update_unknown_record(self, client):
        """
        User tries to update a record which does not exist.
        :param client:
        :return:
        """
        token = get_access_token()
        headers = {"Authorization": f"Bearer {token}"}
        payload = {
            "payload": {
                "modelName": "test_db.customers",
                "id": 1000,  # ID that does not exist
                "saveInput": [
                    {
                        "name": "test1",
                        "dob": date(2004, 6, 12).isoformat(),
                        "email": "test1@mail.com",
                        "phone_no": "741852",
                        "address": "Hyd",
                        "zip_code": "555",
                        "status": "alive",
                        "is_active": True,
                    }
                ],
            }
        }

        response = client.post("/save", json=payload, headers=headers)
        res = response.json()

        assert response.status_code == 400
        assert res["error"] == "Not yet record, 1000"
        assert res["code"] == "GA-020"

    def test_user_active_boolean_field_set_false(self, client):
        """
        Scenario where Boolean value (False) is not
        accounted as no value by user.
        """
        token = get_access_token()
        headers = {"Authorization": f"Bearer {token}"}
        payload = {
            "payload": {
                "modelName": "test_db.customers",
                "id": None,
                "saveInput": [
                    {
                        "name": "test1",
                        "dob": date(2004, 6, 12).isoformat(),
                        "email": "test1@mail.com",
                        "phone_no": "741852",
                        "address": "Hyd",
                        "zip_code": "555",
                        "status": "alive",
                        "is_active": False,
                    }
                ],
            }
        }

        response = client.post("/save", json=payload, headers=headers)
        res = response.json()

        assert response.status_code == 200
        assert res["message"] == "saved successfully"
        assert res["data"] == [1]

    def test_no_header(self, client, db_session):
        """
        No header
        :param client:
        :return:
        """
        payload = {
            "payload": {
                "modelName": "test_db.customers",
                "id": None,
                "saveInput": [
                    {
                        "name": "test1",
                        "dob": date(2004, 6, 12).isoformat(),
                        "email": "test1@mail.com",
                        "phone_no": "741852",
                        "address": "Hyd",
                        "zip_code": "555",
                        "status": "alive",
                    },
                    {
                        "name": "test2",
                        "dob": date(2004, 5, 22).isoformat(),
                        "email": "test1@mail.com",
                        "phone_no": "852147",
                        "address": "Bombay",
                        "zip_code": "777",
                        "status": "alive",
                    },
                ],
            }
        }

        response = client.post("/save", json=payload)
        res = response.json()

        assert response.status_code == 400
        assert res["detail"]["error"] == "Authorization token is missing or invalid."
        assert res["detail"]["code"] == "GA-020"

    def test_invalid_token_format(self, client, db_session):
        """
        Invalid header / token format
        :param client:
        :return:
        """
        token = get_access_token()

        headers = {"Authorization": f"HAHAHA {token}"}

        payload = {
            "payload": {
                "modelName": "test_db.customers",
                "id": None,
                "saveInput": [
                    {
                        "name": "test1",
                        "dob": date(2004, 6, 12).isoformat(),
                        "email": "test1@mail.com",
                        "phone_no": "741852",
                        "address": "Hyd",
                        "zip_code": "555",
                        "status": "alive",
                    },
                    {
                        "name": "test2",
                        "dob": date(2004, 5, 22).isoformat(),
                        "email": "test1@mail.com",
                        "phone_no": "852147",
                        "address": "Bombay",
                        "zip_code": "777",
                        "status": "alive",
                    },
                ],
            }
        }

        response = client.post("/save", json=payload, headers=headers)
        res = response.json()

        assert response.status_code == 400
        assert res["detail"]["error"] == "Authorization token is missing or invalid."
        assert res["detail"]["code"] == "GA-020"
