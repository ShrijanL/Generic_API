# Helper funcs for test cases


def create_test_records(session, *instances):
    """
    Inserts dependent records for test case
    """

    session.add_all(instances)
    session.commit()

    return instances
