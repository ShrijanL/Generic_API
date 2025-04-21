from fastapi import FastAPI
from fastapi.testclient import TestClient


class TestFetchAPI:

    def test_fetch_success(self):

        payload = {
            "payload": {
                "modelName": "std_prj.student_app_customer",
                "fields": ["id", "name", "phone_no", "class_student"],
                "filters": [{"operator": "eq", "name": "class_student", "value": [15]}],
                "joins": [
                    {
                        "student_app_customer.class_student": "std_prj.student_app_studentclass.id"
                    }
                ],
                "pageNumber": 1,
                "pageSize": 3,
                "sort": {"field": "name", "order_by": "asc"},
            }
        }

        pass
