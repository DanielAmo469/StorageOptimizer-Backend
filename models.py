from sqlalchemy import Column, Integer, String, DateTime, Enum, ForeignKey
from sqlalchemy.orm import relationship, Session
from database import Base
from datetime import datetime
import enum

class Role(enum.Enum):
    manager = "manager"
    viewonly = "viewonly"


class ActionType(enum.Enum):
    moved_to_archive = "moved_to_archive"
    restored_from_archive = "restored_from_archive"


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


class FileMovement(Base):
    __tablename__ = "file_movements"

    id = Column(Integer, primary_key=True, index=True)
    full_path = Column(String, index=True)
    destination_path = Column(String)
    creation_time = Column(DateTime)
    last_access_time = Column(DateTime)
    last_modified_time = Column(DateTime)
    file_size = Column(Integer)
    timestamp = Column(DateTime, default=datetime.utcnow)
    action_type = Column(Enum(ActionType), nullable=False)
