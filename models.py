from sqlalchemy import JSON, Boolean, Column, Integer, String, DateTime, Enum, ForeignKey
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


class VolumeScanLog(Base):
    __tablename__ = "volume_scan_log"

    id = Column(Integer, primary_key=True, index=True)
    share_name = Column(String, index=True)
    volume_name = Column(String, index=True)
    timestamp = Column(DateTime, default=datetime.utcnow)
    triggered_by_user = Column(Boolean, default=False)
    files_scanned = Column(Integer, default=0)
    files_archived = Column(Integer, default=0)
    files_restored = Column(Integer, default=0)
    filters_used = Column(JSON, nullable=True)