from typing import List
from datetime import timedelta
from fastapi import FastAPI, Depends, HTTPException, status
from sqlalchemy.orm import Session
from passlib.hash import bcrypt
from jose import JWTError, jwt
from fastapi.security import OAuth2PasswordRequestForm
from fastapi.middleware.cors import CORSMiddleware



from database import SessionLocal, engine, Base, get_db
from models import PendingUser, Role, User
from schemas import BaseResponse, RegistrationRequests, UserCreate, UserValues
from services import get_user_id_by_username, verify_manager
from auth import ALGORITHM, SECRET_KEY, create_access_token, get_current_user


Base.metadata.create_all(bind=engine)

app = FastAPI()



@app.post("/register", response_model=BaseResponse)
def register_user(user: UserCreate, db: Session = Depends(get_db)):
    if not user.is_valid_password:
        raise HTTPException(status_code=422, detail="Password must be at least 8 characters and contain at least one uppercase letter.")

    if user.password != user.verify_password:
        raise HTTPException(status_code=400, detail="Passwords do not match.")

    existing_user = db.query(User).filter(User.email == user.email).first()
    existing_pending_user = db.query(PendingUser).filter(PendingUser.email == user.email).first()
    if existing_user:
        raise HTTPException(status_code=400, detail="Email already registered.")
    elif existing_pending_user:
        raise HTTPException(status_code=400, detail="Your registration is still pending approval.")

    if db.query(User).filter(User.username == user.username).first() or db.query(PendingUser).filter(PendingUser.username == user.username).first():
        raise HTTPException(status_code=400, detail="Username already taken.")

    hashed_password = bcrypt.hash(user.password)
    new_pending_user = PendingUser(
        username=user.username,
        email=user.email,
        hashed_password=hashed_password,
        registration_request_description=user.registration_request_description
    )
    db.add(new_pending_user)
    db.commit()
    db.refresh(new_pending_user)

    return {"message": "User registration request created", "user_id": new_pending_user.id}

@app.post("/login")
def login(form_data: OAuth2PasswordRequestForm = Depends(), db: Session = Depends(get_db)):
    user = db.query(User).filter(User.email == form_data.username).first()

    if not user or not bcrypt.verify(form_data.password, user.hashed_password):
        pending_user = db.query(PendingUser).filter(PendingUser.email == form_data.username).first()
        if pending_user and bcrypt.verify(form_data.password, pending_user.hashed_password):
            raise HTTPException(status_code=403, detail="Your account is still pending approval.")
        raise HTTPException(status_code=400, detail="Invalid credentials")

    token = create_access_token(
        sub=user.username,
        user_id=user.id,
        expires_delta=timedelta(minutes=60)
    )

    return {"access_token": token, "token_type": "bearer"}

@app.get("/me", response_model=UserValues)
def get_user_info(current_user=Depends(get_current_user)):
    return {
        "user_id": current_user.id,
        "username": current_user.username,
        "email": current_user.email,
        "date_created": current_user.date_created
    }


@app.get("/registration-requests", response_model=List[RegistrationRequests])
def get_registration_requests(
    db: Session = Depends(get_db), 
    user: User = Depends(verify_manager)
):
    pending_users = db.query(PendingUser).all()
    return [
        RegistrationRequests(
            username=pending_user.username,
            registration_request_description=pending_user.registration_request_description
        )
        for pending_user in pending_users
    ]

@app.post("/approve-registration/{pending_user_id}")
def approve_registration(
    pending_user_id: int,
    db: Session = Depends(get_db),
    current_user: User = Depends(verify_manager)
):
    pending_user = db.query(PendingUser).filter(PendingUser.id == pending_user_id).first()
    
    if not pending_user:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail="Pending user not found"
        )
    
    approved_user = User(
        username=pending_user.username,
        email=pending_user.email,
        hashed_password=pending_user.hashed_password,
        role=Role.viewonly,
        date_created=pending_user.date_created
    )
    db.add(approved_user)
    db.delete(pending_user)
    db.commit()
    db.refresh(approved_user)
    return {"message": "User approved", "user_id": approved_user.id}


@app.delete("/deny-registration/{pending_user_id}")
def deny_registration(
    pending_user_id: int,
    db: Session = Depends(get_db),
    current_user: User = Depends(verify_manager)
):
    pending_user = db.query(PendingUser).filter(PendingUser.id == pending_user_id).first()
    
    if not pending_user:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail="Pending user not found"
        )

    db.delete(pending_user)
    db.commit()
    return {"message": "User denied and removed from pending registrations", "user_id": pending_user_id}

app.add_middleware(
    CORSMiddleware,
    allow_origins=["http://localhost:3000"],  # React app running on port 3000
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

if __name__ == "__main__":
    import uvicorn
    uvicorn.run("main:app", host="127.0.0.1", port=8000, reload=True)
