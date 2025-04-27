import json
import os
from fastapi import Depends, HTTPException, status
from auth import get_current_user
from database import SessionLocal, get_db
from models import Role, User
from sqlalchemy.orm import Session

from schemas import ArchiveFilterRequest



def get_user_id_by_username(username: str, db: Session) -> int:
    user = db.query(User).filter(User.username == username).first()
    if user:
        return user.id
    else:
        return 0
    

def get_user_by_id(user_id: int, db: Session):
    user = db.query(User).filter(User.ID == user_id).first()
    if not user:
        raise HTTPException(status_code=401, detail="User not found")
    return user


def verify_manager(user: User = Depends(get_current_user), db: Session = Depends(get_db)):
    if user.role != Role.manager:
        raise HTTPException(
            status_code=status.HTTP_403_FORBIDDEN, 
            detail="You do not have permission to view registration requests."
        )
    return user

def verify_viewonly(user:User =  Depends(get_current_user)):
    if user.role !="viewonly":
        raise HTTPException(
            status_code=status.HTTP_403_FORBIDDEN,
            detail="Access forbidden: View and Managers only"
        )
    return user

# Build filter dictionary from request
def build_filters_from_request(filter_request: ArchiveFilterRequest) -> dict:
    return {
        "file_type": filter_request.file_type or [],
        "date_filters": filter_request.date_filters.dict() if filter_request.date_filters else {},
        "min_size": filter_request.min_size,
        "max_size": filter_request.max_size,
    }

def load_settings():
    with open("settings.json", "r") as f:
        return json.load(f)
    
def get_settings_for_mode(mode: str = "default") -> dict:
    settings_path = os.path.join(os.path.dirname(__file__), "settings.json")

    with open(settings_path, "r") as f:
        settings = json.load(f)

    all_modes = settings.get("modes", {})
    mode_config = all_modes.get(mode)

    if not mode_config:
        print(f"Mode '{mode}' not found in settings.json. Falling back to 'default'.")
        mode_config = all_modes.get("default", {})

    return mode_config