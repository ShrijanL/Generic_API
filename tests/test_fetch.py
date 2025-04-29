from factory_recs import make_class, make_customer
from fixtures import engine_controller, db_session, client
from utils import create_test_records, get_access_token

usage = engine_controller

class TestFetchAPI:

    def test_fetch_success(self, client, db_session):
        """
        User fetches records successfully.
        """

        token = get_access_token()

        customer1 = make_customer(id=1, name="Test1", class_id=1)
        class1 = make_class(id=1)

        create_test_records(db_session, class1, customer1)

        headers = {"Authorization": f"Bearer {token}"}

        payload = {
            "payload": {
                "modelName": "test_db.customers",
                "fields": ["id", "name", "phone_no", "class_student"],
                "filters": [{"operator": "eq", "name": "class_student", "value": [1]}],
                "joins": [{"customers.class_student": "test_db.student_classes.id"}],
                "pageNumber": 1,
                "pageSize": 3,
                "sort": {"field": "name", "order_by": "asc"},
            }
        }

        response = client.post("/fetch", json=payload, headers=headers)
        res = response.json()

        assert response.status_code == 200
        assert res["message"] == "Fetch Success"
        assert res["data"] == [
            {"id": 1, "name": "Test1", "phone_no": "1234657890", "class_student": 1}
        ]

    def test_fetch_success_in_operator(self, client, db_session):
        """
        User fetches records successfully using in operator.
        """

        token = get_access_token()

        class1 = make_class(id=1)
        class2 = make_class(id=2, name="Class 2", count=100)
        customer1 = make_customer(id=1, phone="1234567890", class_id=1)
        customer2 = make_customer(id=2, name="Name 2", phone="7418529630", class_id=2)

        create_test_records(db_session, class1, customer1)
        create_test_records(db_session, class2, customer2)

        headers = {"Authorization": f"Bearer {token}"}

        payload = {
            "payload": {
                "modelName": "test_db.customers",
                "fields": ["id", "name", "phone_no", "class_student"],
                "filters": [
                    {
                        "operator": "in",
                        "name": "phone_no",
                        "value": ["1234567890", "7418529630"],
                    }
                ],
                "joins": [{"customers.class_student": "test_db.student_classes.id"}],
                "pageNumber": 1,
                "pageSize": 3,
                "sort": {"field": "name", "order_by": "asc"},
            }
        }

        response = client.post("/fetch", json=payload, headers=headers)
        res = response.json()

        assert response.status_code == 200
        assert res["message"] == "Fetch Success"
        assert res["data"] == [
            {"id": 2, "name": "Name 2", "phone_no": "7418529630", "class_student": 2},
            {"id": 1, "name": "Name1", "phone_no": "1234567890", "class_student": 1},
        ]

    def test_fetch_success_not_operator(self, client, db_session):
        """
        User fetches records successfully using not operator.
        """

        token = get_access_token()

        customer1 = make_customer(id=1, phone="1234567890")
        customer2 = make_customer(id=2, name="Name 2", phone="7418529630")

        create_test_records(db_session, customer1, customer2)

        headers = {"Authorization": f"Bearer {token}"}

        payload = {
            "payload": {
                "modelName": "test_db.customers",
                "fields": ["id", "name", "phone_no"],
                "filters": [
                    {
                        "operator": "not",
                        "name": "phone_no",
                        "value": ["7418529630"],
                    }
                ],
                "pageNumber": 1,
                "pageSize": 3,
                "sort": {"field": "name", "order_by": "asc"},
            }
        }

        response = client.post("/fetch", json=payload, headers=headers)
        res = response.json()

        assert response.status_code == 200
        assert res["message"] == "Fetch Success"
        assert res["data"] == [{"id": 1, "name": "Name1", "phone_no": "1234567890"}]

    def test_fetch_success_gt_operator(self, client, db_session):
        """
        User fetches records successfully using not operator.
        """

        token = get_access_token()

        customer1 = make_customer(id=1)
        customer2 = make_customer(id=2, name="Name2", phone="7418529630", experience=15)
        customer3 = make_customer(id=3, name="Name3", experience=19)

        create_test_records(db_session, customer1, customer2, customer3)

        headers = {"Authorization": f"Bearer {token}"}

        payload = {
            "payload": {
                "modelName": "test_db.customers",
                "fields": ["id", "name", "experience"],
                "filters": [
                    {
                        "operator": "gt",
                        "name": "experience",
                        "value": [10],
                    }
                ],
                "pageNumber": 1,
                "pageSize": 3,
                "sort": {"field": "name", "order_by": "asc"},
            }
        }

        response = client.post("/fetch", json=payload, headers=headers)
        res = response.json()

        assert response.status_code == 200
        assert res["message"] == "Fetch Success"
        assert res["data"] == [
            {"id": 2, "name": "Name2", "experience": 15},
            {"id": 3, "name": "Name3", "experience": 19},
        ]

    def test_fetch_success_gt_operator_2(self, client, db_session):
        """
        User fetches records successfully using not operator.
        """

        token = get_access_token()

        customer1 = make_customer(id=1)
        customer2 = make_customer(id=2, name="Name2", phone="7418529630", experience=15)
        customer3 = make_customer(id=3, name="Name3", experience=19)

        create_test_records(db_session, customer1, customer2, customer3)

        headers = {"Authorization": f"Bearer {token}"}

        payload = {
            "payload": {
                "modelName": "test_db.customers",
                "fields": ["id", "name", "experience"],
                "filters": [
                    {
                        "operator": "gt",
                        "name": "experience",
                        "value": [100],
                    }
                ],
                "pageNumber": 1,
                "pageSize": 3,
                "sort": {"field": "name", "order_by": "asc"},
            }
        }

        response = client.post("/fetch", json=payload, headers=headers)
        res = response.json()

        assert response.status_code == 200
        assert res["message"] == "Fetch Success"
        assert res["data"] == []

    def test_fetch_success_like_operator(self, client, db_session):
        """
        User fetches records successfully using not operator.
        """

        token = get_access_token()

        customer1 = make_customer(id=9, address="Delhi")
        customer2 = make_customer(id=10, name="Name2", address="hyderabad")
        customer3 = make_customer(id=11, name="Name3", address="HYDERABAD")

        create_test_records(db_session, customer1, customer2, customer3)

        headers = {"Authorization": f"Bearer {token}"}

        payload = {
            "payload": {
                "modelName": "test_db.customers",
                "fields": ["id", "name", "address"],
                "filters": [
                    {
                        "operator": "like",
                        "name": "address",
                        "value": ["%HYD%"],
                    }
                ],
                "pageNumber": 1,
                "pageSize": 3,
                "sort": {"field": "name", "order_by": "asc"},
            }
        }

        response = client.post("/fetch", json=payload, headers=headers)
        res = response.json()

        assert response.status_code == 200
        assert res["message"] == "Fetch Success"
        assert res["data"] == [
            {"id": 10, "name": "Name2", "address": "hyderabad"},
            {"id": 11, "name": "Name3", "address": "HYDERABAD"},
        ]

    def test_fetch_without_db_name(self, client, db_session):
        """
        No db in modelName
        """

        token = get_access_token()

        headers = {"Authorization": f"Bearer {token}"}

        payload = {
            "payload": {
                "modelName": "customers",
                "fields": ["id", "name", "phone_no", "class_student"],
                "filters": [{"operator": "eq", "name": "class_student", "value": [1]}],
                "pageNumber": 1,
                "pageSize": 3,
                "sort": {"field": "name", "order_by": "asc"},
            }
        }

        response = client.post("/fetch", json=payload, headers=headers)
        res = response.json()

        assert response.status_code == 400
        assert (
            res["error"]
            == "Value error, modelName must be in format as 'db.table'.('payload', 'modelName')"
        )
        assert res["code"] == "GA-001"

    def test_fetch_incorrect_db_name(self, client, db_session):
        """
        Incorrect db name in modelName
        """

        token = get_access_token()

        headers = {"Authorization": f"Bearer {token}"}

        payload = {
            "payload": {
                "modelName": "HAHA.customers",
                "fields": ["id", "name", "phone_no", "class_student"],
                "filters": [{"operator": "eq", "name": "class_student", "value": [1]}],
                "pageNumber": 1,
                "pageSize": 3,
                "sort": {"field": "name", "order_by": "asc"},
            }
        }

        response = client.post("/fetch", json=payload, headers=headers)
        res = response.json()

        assert response.status_code == 400
        assert res["error"] == "DB HAHA not found in config."
        assert res["code"] == "GA-008"

    def test_fetch_incorrect_table(self, client, db_session):
        """
        Incorrect table in modelName
        """

        token = get_access_token()

        headers = {"Authorization": f"Bearer {token}"}

        payload = {
            "payload": {
                "modelName": "test_db.HAHA",
                "fields": ["id", "name", "phone_no", "class_student"],
                "filters": [{"operator": "eq", "name": "class_student", "value": [1]}],
                "pageNumber": 1,
                "pageSize": 3,
                "sort": {"field": "name", "order_by": "asc"},
            }
        }

        response = client.post("/fetch", json=payload, headers=headers)
        res = response.json()

        assert response.status_code == 400
        assert res["error"] == "HAHA not found in DB test_db"
        assert res["code"] == "GA-010"

    def test_payload_missing_field_property(self, client, db_session):
        """
        Payload is missing a field
        """

        token = get_access_token()

        headers = {"Authorization": f"Bearer {token}"}

        payload = {
            "payload": {
                "modelName": "test_db.customers",
                # "fields": ["id", "name", "phone_no", "class_student"],
                "filters": [{"operator": "eq", "name": "class_student", "value": [1]}],
                "joins": [{"customers.class_student": "test_db.student_classes.id"}],
                "pageNumber": 1,
                "pageSize": 3,
                "sort": {"field": "name", "order_by": "asc"},
            }
        }

        response = client.post("/fetch", json=payload, headers=headers)
        res = response.json()

        assert response.status_code == 400
        assert res["error"] == "Field required('payload', 'fields')"
        assert res["code"] == "GA-001"

    def test_no_header(self, client, db_session):
        """
        No header
        """

        payload = {
            "payload": {
                "modelName": "test_db.customers",
                "fields": ["id", "name", "phone_no", "class_student"],
                "filters": [{"operator": "eq", "name": "class_student", "value": [1]}],
                "joins": [{"customers.class_student": "test_db.student_classes.id"}],
                "pageNumber": 1,
                "pageSize": 3,
                "sort": {"field": "name", "order_by": "asc"},
            }
        }

        response = client.post("/fetch", json=payload)
        res = response.json()

        assert response.status_code == 401
        assert res["detail"]["error"] == "Authorization token is missing or invalid."
        assert res["detail"]["code"] == "GA-027"

    def test_invalid_token_format(self, client, db_session):
        """
        Invalid header / token format
        """

        token = get_access_token()

        headers = {"Authorization": f"HAHAHA {token}"}
        payload = {
            "payload": {
                "modelName": "test_db.customers",
                "fields": ["id", "name", "phone_no", "class_student"],
                "filters": [{"operator": "eq", "name": "class_student", "value": [1]}],
                "joins": [{"customers.class_student": "test_db.student_classes.id"}],
                "pageNumber": 1,
                "pageSize": 3,
                "sort": {"field": "name", "order_by": "asc"},
            }
        }

        response = client.post("/fetch", json=payload, headers=headers)
        res = response.json()

        assert response.status_code == 401
        assert res["detail"]["error"] == "Authorization token is missing or invalid."
        assert res["detail"]["code"] == "GA-027"

    def test_extra_field_in_payload(self, client, db_session):
        """
        Extra field in payload
        """

        token = get_access_token()

        headers = {"Authorization": f"Bearer {token}"}

        payload = {
            "payload": {
                "modelName": "test_db.customers",
                "dummy": "YOYO",
                "fields": ["id", "name", "phone_no", "class_student"],
                "filters": [{"operator": "eq", "name": "class_student", "value": [1]}],
                "joins": [{"customers.class_student": "test_db.student_classes.id"}],
                "pageNumber": 1,
                "pageSize": 3,
                "sort": {"field": "name", "order_by": "asc"},
            }
        }

        response = client.post("/fetch", json=payload, headers=headers)
        res = response.json()

        assert response.status_code == 400
        assert res["error"] == "Extra inputs are not permitted('payload', 'dummy')"
        assert res["code"] == "GA-001"

    def test_fetch_incorrect_table_data_type(self, client, db_session):
        """
        Incorrect table in modelName
        """

        token = get_access_token()

        headers = {"Authorization": f"Bearer {token}"}

        payload = {
            "payload": {
                "modelName": 420,
                "fields": ["id", "name", "phone_no", "class_student"],
                "filters": [{"operator": "eq", "name": "class_student", "value": [1]}],
                "pageNumber": 1,
                "pageSize": 3,
                "sort": {"field": "name", "order_by": "asc"},
            }
        }

        response = client.post("/fetch", json=payload, headers=headers)
        res = response.json()

        assert response.status_code == 400
        assert res["error"] == "Input should be a valid string('payload', 'modelName')"
        assert res["code"] == "GA-001"

    def test_invalid_payload_fields_data_type(self, client, db_session):
        """
        Invalid data type for fields
        """

        token = get_access_token()

        headers = {"Authorization": f"Bearer {token}"}

        payload = {
            "payload": {
                "modelName": "test_db.customers",
                "fields": "id",
                "filters": [{"operator": "eq", "name": "class_student", "value": [1]}],
                "joins": [{"customers.class_student": "test_db.student_classes.id"}],
                "pageNumber": 1,
                "pageSize": 3,
                "sort": {"field": "name", "order_by": "asc"},
            }
        }

        response = client.post("/fetch", json=payload, headers=headers)
        res = response.json()

        assert response.status_code == 400
        assert res["error"] == "Input should be a valid list('payload', 'fields')"
        assert res["code"] == "GA-001"

    def test_unknown_fetch_filter_name(self, client, db_session):
        """
        Wrong field name in filters
        """

        token = get_access_token()

        headers = {"Authorization": f"Bearer {token}"}

        payload = {
            "payload": {
                "modelName": "test_db.customers",
                "fields": ["id", "name", "phone_no", "class_student"],
                "filters": [{"operator": "eq", "name": "is_active", "value": ["HAHA"]}],
                "joins": [{"customers.class_student": "test_db.student_classes.id"}],
                "pageNumber": 1,
                "pageSize": 3,
                "sort": {"field": "name", "order_by": "asc"},
            }
        }

        response = client.post("/fetch", json=payload, headers=headers)
        res = response.json()

        assert response.status_code == 400
        assert res["error"] == "Invalid value for filter:[is_active=HAHA]"
        assert res["code"] == "GA-018"

    def test_invalid_sort_field(self, client, db_session):
        """
        Invalid sort field
        """

        token = get_access_token()

        headers = {"Authorization": f"Bearer {token}"}

        payload = {
            "payload": {
                "modelName": "test_db.customers",
                "fields": ["id", "name"],
                "filters": [{"operator": "eq", "name": "class_student", "value": [1]}],
                "joins": [{"customers.class_student": "test_db.student_classes.id"}],
                "pageNumber": 1,
                "pageSize": 3,
                "sort": {"field": "HAHA", "order_by": "asc"},
            }
        }

        response = client.post("/fetch", json=payload, headers=headers)
        res = response.json()

        assert response.status_code == 400
        assert res["error"] == "Invalid field for sort, HAHA"
        assert res["code"] == "GA-019"

    def test_invalid_fields_field(self, client, db_session):
        """
        Invalid sort field
        """

        token = get_access_token()

        headers = {"Authorization": f"Bearer {token}"}

        payload = {
            "payload": {
                "modelName": "test_db.customers",
                "fields": ["id", "name", "HAHA"],
                "filters": [{"operator": "eq", "name": "class_student", "value": [1]}],
                "joins": [{"customers.class_student": "test_db.student_classes.id"}],
                "pageNumber": 1,
                "pageSize": 3,
                "sort": {"field": "name", "order_by": "asc"},
            }
        }

        response = client.post("/fetch", json=payload, headers=headers)
        res = response.json()

        assert response.status_code == 400
        assert res["error"] == "Invalid field in fields, HAHA"
        assert res["code"] == "GA-016"

    def test_invalid_filter_format(self, client, db_session):
        """
        Invalid filters format
        """

        token = get_access_token()

        headers = {"Authorization": f"Bearer {token}"}

        payload = {
            "payload": {
                "modelName": "test_db.customers",
                "fields": ["id", "name"],
                "filters": ["eq", "class_student", [1]],
                "joins": [{"customers.class_student": "test_db.student_classes.id"}],
                "pageNumber": 1,
                "pageSize": 3,
                "sort": {"field": "name", "order_by": "asc"},
            }
        }

        response = client.post("/fetch", json=payload, headers=headers)
        res = response.json()

        assert response.status_code == 400
        assert (
            res["error"]
            == "Input should be a valid dictionary or instance of FetchFilter('payload', 'filters', 0)"
        )
        assert res["code"] == "GA-001"

    def test_invalid_fetch_filter_operator(self, client, db_session):
        """
        User fetches with invalid filter operator.
        """

        token = get_access_token()

        headers = {"Authorization": f"Bearer {token}"}

        payload = {
            "payload": {
                "modelName": "test_db.customers",
                "fields": ["id", "name", "phone_no", "class_student"],
                "filters": [
                    {"operator": "haha", "name": "class_student", "value": [1]}
                ],
                "joins": [{"customers.class_student": "test_db.student_classes.id"}],
                "pageNumber": 1,
                "pageSize": 3,
                "sort": {"field": "name", "order_by": "asc"},
            }
        }

        response = client.post("/fetch", json=payload, headers=headers)
        res = response.json()

        assert response.status_code == 400
        assert (
            res["error"]
            == "Input should be 'eq', 'in', 'not', 'gt', 'lt', 'like' or 'ilike'('payload', 'filters', 0, 'operator')"
        )
        assert res["code"] == "GA-001"

    def test_invalid_fetch_filter_name_data_type(self, client, db_session):
        """
        User fetches with invalid data type for filter name.
        """

        token = get_access_token()

        headers = {"Authorization": f"Bearer {token}"}

        payload = {
            "payload": {
                "modelName": "test_db.customers",
                "fields": ["id", "name", "phone_no", "class_student"],
                "filters": [{"operator": "eq", "name": 123456, "value": [1]}],
                "joins": [{"customers.class_student": "test_db.student_classes.id"}],
                "pageNumber": 1,
                "pageSize": 3,
                "sort": {"field": "name", "order_by": "asc"},
            }
        }

        response = client.post("/fetch", json=payload, headers=headers)
        res = response.json()

        assert response.status_code == 400
        assert (
            res["error"]
            == "Input should be a valid string('payload', 'filters', 0, 'name')"
        )
        assert res["code"] == "GA-001"

    def test_invalid_page_size_data_type(self, client, db_session):
        """
        User fetches with invalid data type for page size.
        """

        token = get_access_token()

        headers = {"Authorization": f"Bearer {token}"}

        payload = {
            "payload": {
                "modelName": "test_db.customers",
                "fields": ["id", "name", "phone_no", "class_student"],
                "filters": [{"operator": "eq", "name": "class_student", "value": [1]}],
                "joins": [{"customers.class_student": "test_db.student_classes.id"}],
                "pageSize": "haha",
                "pageNumber": 3,
                "sort": {"field": "name", "order_by": "asc"},
            }
        }

        response = client.post("/fetch", json=payload, headers=headers)
        res = response.json()

        assert response.status_code == 400
        assert (
            res["error"]
            == "Input should be a valid integer, unable to parse string as an integer('payload', 'pageSize')"
        )
        assert res["code"] == "GA-001"

    def test_invalid_page_num_data_type(self, client, db_session):
        """
        User fetches with invalid data type for page number.
        """

        token = get_access_token()

        headers = {"Authorization": f"Bearer {token}"}

        payload = {
            "payload": {
                "modelName": "test_db.customers",
                "fields": ["id", "name", "phone_no", "class_student"],
                "filters": [{"operator": "eq", "name": "class_student", "value": [1]}],
                "joins": [{"customers.class_student": "test_db.student_classes.id"}],
                "pageSize": "haha",
                "pageNumber": 3,
                "sort": {"field": "name", "order_by": "asc"},
            }
        }

        response = client.post("/fetch", json=payload, headers=headers)
        res = response.json()

        assert response.status_code == 400
        assert (
            res["error"]
            == "Input should be a valid integer, unable to parse string as an integer('payload', 'pageSize')"
        )
        assert res["code"] == "GA-001"

    def test_invalid_value_for_sort_order_by(self, client, db_session):
        """
        User fetches with invalid value for sort orderby.
        """

        token = get_access_token()

        headers = {"Authorization": f"Bearer {token}"}

        payload = {
            "payload": {
                "modelName": "test_db.customers",
                "fields": ["id", "name", "phone_no", "class_student"],
                "filters": [{"operator": "eq", "name": "class_student", "value": [1]}],
                "joins": [{"customers.class_student": "test_db.student_classes.id"}],
                "pageSize": 3,
                "pageNumber": 3,
                "sort": {"field": "name", "order_by": "haha"},
            }
        }

        response = client.post("/fetch", json=payload, headers=headers)
        res = response.json()

        assert response.status_code == 400
        assert (
            res["error"]
            == "Input should be 'asc' or 'desc'('payload', 'sort', 'order_by')"
        )
        assert res["code"] == "GA-001"

    def test_multiple_values_eq_operator(self, client, db_session):
        """
        Filter operator is eq and multiple values are given.
        """

        token = get_access_token()

        customer1 = make_customer(id=1, name="Test1", class_id=1)
        class1 = make_class(id=1)

        create_test_records(db_session, class1, customer1)

        headers = {"Authorization": f"Bearer {token}"}

        payload = {
            "payload": {
                "modelName": "test_db.customers",
                "fields": ["id", "name", "phone_no", "class_student"],
                "filters": [
                    {"operator": "eq", "name": "class_student", "value": [1, 2, 3]}
                ],
                "joins": [{"customers.class_student": "test_db.student_classes.id"}],
                "pageNumber": 1,
                "pageSize": 3,
                "sort": {"field": "name", "order_by": "asc"},
            }
        }

        response = client.post("/fetch", json=payload, headers=headers)
        res = response.json()

        assert response.status_code == 400
        assert (
            res["error"]
            == "Value error, Multiple filters not supported('payload', 'filters')"
        )
        assert res["code"] == "GA-001"

    def test_invalid_operation(self, client, db_session):
        """
        filters have invalid operation.
        """

        token = get_access_token()

        customer1 = make_customer(id=1, name="Test1", class_id=1)
        class1 = make_class(id=1)

        create_test_records(db_session, class1, customer1)

        headers = {"Authorization": f"Bearer {token}"}

        payload = {
            "payload": {
                "modelName": "test_db.customers",
                "fields": ["id", "name", "phone_no", "class_student"],
                "filters": [
                    {
                        "operator": "eq",
                        "name": "class_student",
                        "value": [1],
                        "operation": "not",
                    }
                ],
                "joins": [{"customers.class_student": "test_db.student_classes.id"}],
                "pageNumber": 1,
                "pageSize": 3,
                "sort": {"field": "name", "order_by": "asc"},
            }
        }

        response = client.post("/fetch", json=payload, headers=headers)
        res = response.json()

        assert response.status_code == 400
        assert (
            res["error"]
            == "Input should be 'or' or 'and'('payload', 'filters', 0, 'operation')"
        )
        assert res["code"] == "GA-001"

    def test_negative_pagesize(self, client):
        """
        page size is negative.
        """

        token = get_access_token()

        headers = {"Authorization": f"Bearer {token}"}

        payload = {
            "payload": {
                "modelName": "test_db.customers",
                "fields": ["id", "name", "phone_no", "class_student"],
                "filters": [{"operator": "eq", "name": "class_student", "value": [1]}],
                "joins": [{"customers.class_student": "test_db.student_classes.id"}],
                "pageNumber": 1,
                "pageSize": -10,
                "sort": {"field": "name", "order_by": "asc"},
            }
        }

        response = client.post("/fetch", json=payload, headers=headers)
        res = response.json()

        assert response.status_code == 400
        assert (
            res["error"]
            == "Input should be greater than or equal to 1('payload', 'pageSize')"
        )
        assert res["code"] == "GA-001"

    def test_sort_invalid_format(self, client):
        """
        sort is in invalid format.
        """

        token = get_access_token()

        headers = {"Authorization": f"Bearer {token}"}

        payload = {
            "payload": {
                "modelName": "test_db.customers",
                "fields": ["id", "name", "phone_no", "class_student"],
                "filters": [{"operator": "eq", "name": "class_student", "value": [1]}],
                "joins": [{"customers.class_student": "test_db.student_classes.id"}],
                "pageNumber": 1,
                "pageSize": 10,
                "sort": ["name", "asc"],
            }
        }

        response = client.post("/fetch", json=payload, headers=headers)
        res = response.json()

        assert response.status_code == 400
        assert (
            res["error"]
            == "Input should be a valid dictionary or instance of FetchSort('payload', 'sort')"
        )
        assert res["code"] == "GA-001"

    def test_sort_extra_keys(self, client):
        """
        Extra keys in sort
        """

        token = get_access_token()

        headers = {"Authorization": f"Bearer {token}"}

        payload = {
            "payload": {
                "modelName": "test_db.customers",
                "fields": ["id", "name", "phone_no", "class_student"],
                "filters": [{"operator": "eq", "name": "class_student", "value": [1]}],
                "joins": [{"customers.class_student": "test_db.student_classes.id"}],
                "pageNumber": 1,
                "pageSize": 10,
                "sort": {"field": "name", "order_by": "desc", "abc": 123},
            }
        }

        response = client.post("/fetch", json=payload, headers=headers)
        res = response.json()

        assert response.status_code == 400
        assert (
            res["error"] == "Extra inputs are not permitted('payload', 'sort', 'abc')"
        )
        assert res["code"] == "GA-001"

    def test_invalid_distinct_value(self, client):
        """
        Invalid distinct value.
        """

        token = get_access_token()

        headers = {"Authorization": f"Bearer {token}"}

        payload = {
            "payload": {
                "modelName": "test_db.customers",
                "fields": ["id", "name", "phone_no", "class_student"],
                "filters": [{"operator": "eq", "name": "class_student", "value": [1]}],
                "joins": [{"customers.class_student": "test_db.student_classes.id"}],
                "pageNumber": 1,
                "pageSize": 10,
                "sort": {"field": "name", "order_by": "desc"},
                "distinct": 123,
            }
        }

        response = client.post("/fetch", json=payload, headers=headers)
        res = response.json()

        assert response.status_code == 400
        assert (
            res["error"]
            == "Input should be a valid boolean, unable to interpret input('payload', 'distinct')"
        )
        assert res["code"] == "GA-001"

    def test_token_is_invalid(self, client):
        """
        Token is invalid.
        """

        token = "ABCD.EFGH.HAHA"
        headers = {"Authorization": f"Bearer {token}"}

        payload = {
            "payload": {
                "modelName": "test_db.customers",
                "fields": ["id", "name", "phone_no", "class_student"],
                "filters": [{"operator": "eq", "name": "class_student", "value": [1]}],
                "pageNumber": 1,
                "pageSize": 10,
                "sort": {"field": "name", "order_by": "desc"},
            }
        }

        response = client.post("/fetch", json=payload, headers=headers)
        res = response.json()

        assert response.status_code == 400
        assert (
            res["detail"]["error"]
            == "Invalid header string: 'utf-8' codec can't decode byte 0x83 in position 2: invalid start byte"
        )
        assert res["detail"]["code"] == "GA-026"
