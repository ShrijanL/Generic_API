import pytest
from fixtures import engine_controller, db_session, client


class TestGenericLogin:

    def test_login_success(self, client):
        """
        User login is success
        :param client:
        :return:
        """
        payload = {"payload": {"email": "admin@gmail.com", "password": "admin123"}}

        response = client.post("/login", json=payload)
        res = response.json()

        assert response.status_code == 200
        assert "access" in res["data"]
        assert "refresh" in res["data"]

        assert res["message"] == "Tokens are generated."

    def test_extra_field_in_payload(self, client):
        """
        Payload contains extra field.
        :param client:
        :return:
        """
        payload = {
            "payload": {
                "email": "admin@gmail.com",
                "password": "admin123",
                "dummy": "Hello",
            }
        }

        response = client.post("/login", json=payload)
        res = response.json()

        assert response.status_code == 400
        assert res["code"] == "GA-..."
        assert res["error"] == "Extra inputs are not permitted('payload', 'dummy')"

    def test_missing_field_in_payload(self, client):
        """
        Payload contains extra field.
        :param client:
        :return:
        """
        payload = {
            "payload": {
                "email": "admin@gmail.com",
                # "password" : "admin123",
            }
        }

        response = client.post("/login", json=payload)
        res = response.json()

        assert response.status_code == 400
        assert res["code"] == "GA-..."
        assert res["error"] == "Field required('payload', 'password')"
