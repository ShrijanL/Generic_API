# Helper funcs for test cases
from api.services import create_tokens


def create_test_records(session, *instances):
    """
    Inserts dependent records for test case
    """
    for rec in instances:
        try:
            session.add(rec)
            session.commit()  # Commit each record one by one
        except Exception as e:
            print(f"Failed to insert {rec}: {e}")
            session.rollback()  # Rollback if error occurs
    return instances


def get_access_token():

    access_token = create_tokens(1, require_refresh=False)

    return access_token
