import logging
import os
from fastapi import HTTPException, Depends
from sqlalchemy.orm import Session
from jose import jwt, JWTError
from datetime import datetime, timedelta, timezone
from models import User
from passlib.hash import bcrypt

from database import get_db

logging.basicConfig(level=logging.INFO)


SECRET_KEY = os.getenv("SECRET_KEY", "MeawMeaWaffWaff")
ALGORITHM = os.getenv("ALGORITHM", "HS256")
ACCESS_TOKEN_EXPIRE_MINUTES = 60

logging.info("SECRET_KEY: %s", SECRET_KEY)
logging.info("ALGORITHM: %s", ALGORITHM)

# Define OAuth2PasswordBearer outside the function
from fastapi.security import OAuth2PasswordBearer
oauth2_scheme = OAuth2PasswordBearer(tokenUrl="/login")

# Function to create a JWT token with JSON-serializable data
def create_access_token(sub: str, user_id: int, expires_delta: timedelta = None):
    not_before = datetime.now(timezone.utc)  # Use timezone-aware datetime
    if expires_delta is None:
        expires_delta = timedelta(minutes=60)  # Default expiration duration
    expire = int((datetime.now(timezone.utc) + expires_delta).timestamp())
    to_encode = {
        "sub": sub,
        "user_id": user_id,  # Use "user_id" instead of just "id"
        "nbf": int(not_before.timestamp()),  # Convert to integer
        "exp": expire,
    }
    return jwt.encode(to_encode, SECRET_KEY, ALGORITHM)


# Function to validate a token
def validate_token(token: str):
    SECRET_KEY = os.getenv("SECRET_KEY", "jwt_secret_key12312123")  # Ensure correct secret key
    ALGORITHM = os.getenv("ALGORITHM", "HS256")
    
    logging.info("Decoding with SECRET_KEY: %s", SECRET_KEY)

    try:
        decoded = jwt.decode(token, SECRET_KEY, algorithms=[ALGORITHM])
        return decoded
    except JWTError as e:
        raise HTTPException(status_code=401, detail="Invalid token")

# Function to get the current user from a JWT token
def get_current_user(token: str = Depends(oauth2_scheme), db: Session = Depends(get_db)):
    try:
        # Decode the JWT token
        payload = jwt.decode(token, SECRET_KEY, algorithms=[ALGORITHM])
        user_id = payload.get("user_id")
        if not user_id:
            raise HTTPException(status_code=401, detail="Invalid token")
        
        # Fetch the user from the database
        user = db.query(User).filter(User.id == user_id).first()
        if not user:
            raise HTTPException(status_code=401, detail="User not found")

        return user  # Return the user object
    except JWTError:
        raise HTTPException(status_code=401, detail="Invalid token")
