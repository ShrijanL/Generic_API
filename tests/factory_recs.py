from tests.models import Customer, StudentClass, UserModel
from datetime import date


"""
    In fetch tests, even though for every test case a new session is created and the record instance is used,
    after usage of 1 instance by a session, that object is marked as `detached`.So in next test case,
    SQLAlchemy does not recognize the instance as 'new', So here dynamically new instances are created. 
"""


def make_class(id, name: str = "Class 1", count: int = 25):
    return StudentClass(id=id, name=name, student_count=count)


def make_customer(
    id,
    name: str = "Name1",
    dob: date = date(2003, 4, 27),
    phone: str = "1234657890",
    address: str = "123 Main Street",
    zip_code: str = "123456",
    status: str = "A",
    class_id: int = None,
    is_active: bool = True,
    experience: int = 10,
):
    return Customer(
        id=id,
        name=name,
        dob=dob,
        email=f"{name.lower()}@example.com",
        phone_no=phone,
        address=address,
        zip_code=zip_code,
        status=status,
        class_student=class_id,
        is_active=is_active,
        experience=experience,
    )


def make_user(id, email, password, name):
    return UserModel(id=id, name=name, email=email, password=password)
