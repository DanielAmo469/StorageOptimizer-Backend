from sqlalchemy import Column, Integer, String, DateTime, Enum, ForeignKey
from sqlalchemy.orm import relationship, Session
from database import Base
from datetime import datetime
import enum

class Role(enum.Enum):
    manager = "manager"
    viewonly = "viewonly"

class User(Base):
    __tablename__ = "users"

    id = Column(Integer, primary_key=True, index=True)
    username = Column(String, index=True)
    email = Column(String, unique=True, index=True)
    hashed_password = Column(String)
    role = Column(Enum(Role), nullable=False, default=Role.viewonly)
    date_created = Column(DateTime, default=datetime.utcnow)

class PendingUser(Base):
    __tablename__ = "pending_users"

    id = Column(Integer, primary_key=True, index=True)
    username = Column(String, index=True)
    email = Column(String, unique=True, index=True)
    hashed_password = Column(String)
    registration_request_description = Column(String)
    date_created = Column(DateTime, default=datetime.utcnow)