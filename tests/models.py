# tests/models.py
from sqlalchemy import Column, Integer, String, Date, ForeignKey, Boolean
from sqlalchemy.orm import declarative_base

Base = declarative_base()


class UserModel(Base):
    __tablename__ = "user_model"

    id = Column(Integer, primary_key=True)
    email = Column(String(63), nullable=False)
    password = Column(String(63), nullable=False)
    name = Column(String(63), nullable=False)


class StudentClass(Base):
    __tablename__ = "student_classes"

    id = Column(Integer, primary_key=True)
    name = Column(String(63), nullable=False)
    student_count = Column(Integer)


class Customer(Base):
    __tablename__ = "customers"

    id = Column(Integer, primary_key=True)
    name = Column(String(63), nullable=False)
    dob = Column(Date, nullable=True)
    email = Column(String(63), nullable=False)
    phone_no = Column(String(15), nullable=False)
    address = Column(String(1023), nullable=False)
    zip_code = Column(String(6), nullable=False)
    status = Column(String(5))
    is_active = Column(Boolean, nullable=True)
    experience = Column(Integer, nullable=True)

    class_student = Column(Integer, ForeignKey("student_classes.id"), nullable=True)
