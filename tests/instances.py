from datetime import date

from tests.models import Customer, StudentClass, UserModel

class1 = StudentClass(id=1, name="Class 1", student_count=15)

class2 = StudentClass(id=2, name="Class 2", student_count=25)

class3 = StudentClass(id=3, name="Class 3", student_count=35)

customer1 = Customer(
    id=1,
    name="Name1",
    dob=date(2003, 4, 27),
    email="name1@example.com",
    phone_no="1234567890",
    address="123 Main Street",
    zip_code="123456",
    status="A",
    class_student=1,
    is_active=True,
)

customer2 = Customer(
    id=2,
    name="Name2",
    dob=date(2005, 2, 2),
    email="name2@example.com",
    phone_no="0987654321",
    address="456 Side Street",
    zip_code="654321",
    status="B",
    class_student=2,
    is_active=True,
)

customer3 = Customer(
    id=3,
    name="Name3",
    dob=date(2005, 2, 2),
    email="name3@example.com",
    phone_no="7418529630",
    address="789 Gone Street",
    zip_code="789456",
    status="B",
    class_student=3,
    is_active=True,
)

user1 = UserModel(id=1, email="admin@gmail.com", password="admin123", name="admin")

user2 = UserModel(id=2, email="user@gmail.com", password="user123", name="user two")
