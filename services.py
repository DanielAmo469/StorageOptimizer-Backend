import json
import os
from fastapi import Depends, HTTPException, status
from auth import get_current_user
from database import SessionLocal, get_db
from models import Role, User
from sqlalchemy.orm import Session



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

SETTINGS_FILE = "settings.json"

def load_blacklist():
    if not os.path.exists(SETTINGS_FILE):
        return []
    with open(SETTINGS_FILE, "r") as f:
        data = json.load(f)
    return data.get("blacklist", [])

def save_blacklist(blacklist: list[str]):
    data = {"blacklist": blacklist}
    with open(SETTINGS_FILE, "w") as f:
        json.dump(data, f, indent=2)
    return data