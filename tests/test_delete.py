from factory_recs import make_class, make_customer
from fixtures import engine_controller, db_session, client
from utils import create_test_records, get_access_token

usage = engine_controller

class TestDeleteAPI:

    def test_delete_success(self,client, db_session):
        """
        User deletes records successfully.
        """
        token = get_access_token()

        customer1 = make_customer(id=1, name="Test1", class_id=1)
        class1 = make_class(id=1)
        create_test_records(db_session, class1, customer1)

        headers = {"Authorization": f"Bearer {token}"}

        payload = {
            "payload": {
                "modelName": "test_db.customers",
                "filters":
                    [{
                        "operator": "eq",
                        "name": "id",
                        "value": [1]
                    }
                    ]
            }
        }

        response = client.post("/delete", json=payload, headers=headers)
        res = response.json()

        assert res["message"] == "deleted successfully"
        assert res["data"] == [1]
        assert response.status_code == 200

    def test_delete_not_yet_record(self,client, db_session):
        """
        User deletes a not yet record.
        """
        token = get_access_token()

        headers = {"Authorization": f"Bearer {token}"}

        payload = {
            "payload": {
                "modelName": "test_db.customers",
                "filters":
                    [{
                        "operator": "eq",
                        "name": "id",
                        "value": [1]
                    }
                    ]
            }
        }

        response = client.post("/delete", json=payload, headers=headers)
        res = response.json()

        assert res["error"] == "No records to delete."
        assert res["code"] == "GA-005"
        assert response.status_code == 400

    def test_delete_no_token(self,client, db_session):
        """
        User does not send an access token
        """

        payload = {
            "payload": {
                "modelName": "test_db.customers",
                "filters":
                    [{
                        "operator": "eq",
                        "name": "id",
                        "value": [1]
                    }
                    ]
            }
        }

        response = client.post("/delete", json=payload)
        res = response.json()

        assert res["detail"]["error"] == "Authorization token is missing or invalid."
        assert res["detail"]["code"] == "GA-027"
        assert response.status_code == 401

    def test_delete_1_exists_1_not(self,client, db_session):
        """
        User sends a payload where only 1 rec exists.
        """
        token = get_access_token()

        customer1 = make_customer(id=1, name="Test1", class_id=1)
        class1 = make_class(id=1)
        create_test_records(db_session, class1, customer1)

        headers = {"Authorization": f"Bearer {token}"}

        payload = {
            "payload": {
                "modelName": "test_db.customers",
                "filters":
                    [{
                        "operator": "in",
                        "name": "id",
                        "value": [1,2]
                    }
                    ]
            }
        }

        response = client.post("/delete", json=payload, headers=headers)
        res = response.json()

        assert res["message"] == "deleted successfully"
        assert res["data"] == [1]
        assert response.status_code == 200

    def test_delete_success_in_operator(self,client, db_session):
        """
        User deletes records successfully using in operator.
        """
        token = get_access_token()

        class1 = make_class(id=1)
        customer1 = make_customer(id=1, name="Test1", class_id=1)
        customer2 = make_customer(id=2, name="Test2", class_id=1)

        create_test_records(db_session, class1, customer1,customer2)

        headers = {"Authorization": f"Bearer {token}"}

        payload = {
            "payload": {
                "modelName": "test_db.customers",
                "filters":
                    [{
                        "operator": "in",
                        "name": "id",
                        "value": [1,2]
                    }
                    ]
            }
        }

        response = client.post("/delete", json=payload, headers=headers)
        res = response.json()

        assert res["message"] == "deleted successfully"
        assert res["data"] == [1,2]
        assert response.status_code == 200

    def test_delete_success_gt_operator(self,client, db_session):
        """
        User deletes records successfully using gt operator.
        """
        token = get_access_token()

        class1 = make_class(id=1)
        customer1 = make_customer(id=1, name="Test1", class_id=1, experience=5)
        customer2 = make_customer(id=2, name="Test2", class_id=1, experience=10)

        create_test_records(db_session, class1, customer1,customer2)

        headers = {"Authorization": f"Bearer {token}"}

        payload = {
            "payload": {
                "modelName": "test_db.customers",
                "filters":
                    [{
                        "operator": "gt",
                        "name": "experience",
                        "value": [7]
                    }
                    ]
            }
        }

        response = client.post("/delete", json=payload, headers=headers)
        res = response.json()

        assert res["message"] == "deleted successfully"
        assert res["data"] == [2]
        assert response.status_code == 200

    def test_delete_success_not_operator(self,client, db_session):
        """
        User deletes records successfully using not operator.
        """
        token = get_access_token()

        class1 = make_class(id=1)
        customer1 = make_customer(id=1, name="Test1", class_id=1, experience=5)
        customer2 = make_customer(id=2, name="Test2", class_id=1, experience=10)

        create_test_records(db_session, class1, customer1,customer2)

        headers = {"Authorization": f"Bearer {token}"}

        payload = {
            "payload": {
                "modelName": "test_db.customers",
                "filters":
                    [{
                        "operator": "not",
                        "name": "experience",
                        "value": [5]
                    }
                    ]
            }
        }

        response = client.post("/delete", json=payload, headers=headers)
        res = response.json()

        assert res["message"] == "deleted successfully"
        assert res["data"] == [2]
        assert response.status_code == 200

    def test_delete_success_lt_operator(self,client, db_session):
        """
        User deletes records successfully using lt operator.
        """
        token = get_access_token()

        class1 = make_class(id=1)
        customer1 = make_customer(id=1, name="Test1", class_id=1, experience=5)
        customer2 = make_customer(id=2, name="Test2", class_id=1, experience=10)

        create_test_records(db_session, class1, customer1,customer2)

        headers = {"Authorization": f"Bearer {token}"}

        payload = {
            "payload": {
                "modelName": "test_db.customers",
                "filters":
                    [{
                        "operator": "lt",
                        "name": "experience",
                        "value": [7]
                    }
                    ]
            }
        }

        response = client.post("/delete", json=payload, headers=headers)
        res = response.json()

        assert res["message"] == "deleted successfully"
        assert res["data"] == [1]
        assert response.status_code == 200
