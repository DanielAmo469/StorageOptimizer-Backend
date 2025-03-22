from typing import Dict, List
from datetime import timedelta
from fastapi import FastAPI, Depends, HTTPException, status
from sqlalchemy.orm import Session
from passlib.hash import bcrypt
from jose import JWTError, jwt
from fastapi.security import OAuth2PasswordRequestForm
from fastapi.middleware.cors import CORSMiddleware
from fastapi.staticfiles import StaticFiles
from fastapi.openapi.docs import get_swagger_ui_html





from database import SessionLocal, engine, Base, get_db
from models import PendingUser, Role, User
from schemas import BaseResponse, RegistrationRequests, UserCreate, UserValues
from services import get_user_id_by_username, verify_manager
from auth import ALGORITHM, SECRET_KEY, create_access_token, get_current_user

def create_admin_user(db: Session):
    admin_email = "admin@gmail.com"
    admin_password = "Adminpassword"

    existing_admin = db.query(User).filter(User.email == admin_email).first()
    if not existing_admin:
        hashed_password = bcrypt.hash(admin_password)
        admin_user = User(
            username="admin",
            email=admin_email,
            hashed_password=hashed_password,
            role=Role.manager
        )
        db.add(admin_user)
        db.commit()
        db.refresh(admin_user)
        print(f"Admin user created with email: {admin_email}")
    else:
        print("Admin user already exists.")


Base.metadata.create_all(bind=engine)

db = SessionLocal()
try:
    create_admin_user(db)
finally:
    db.close()

app = FastAPI(docs_url=None, redoc_url=None)


app.mount("/static", StaticFiles(directory="static"), name="static")

@app.get("/docs", include_in_schema=False)
async def custom_swagger_ui_html():
    return get_swagger_ui_html(
        openapi_url="/openapi.json",
        title="Storage Optimizer - Swagger UI",
        swagger_js_url="/static/swagger-ui-bundle.js",  # ✅ local path
        swagger_css_url="/static/swagger-ui.css",        # ✅ local path
    )

@app.get("/openapi.json", include_in_schema=False)
async def custom_openapi():
    return JSONResponse(
        get_openapi(
            title="Storage Optimizer",
            version="1.0.0",
            routes=app.routes
        )
    )


@app.get(app.swagger_ui_oauth2_redirect_url, include_in_schema=False)
async def swagger_ui_redirect():
    return get_swagger_ui_oauth2_redirect_html()


@app.get("/redoc", include_in_schema=False)
async def redoc_html():
    return get_redoc_html(
        openapi_url=app.openapi_url,
        title=app.title + " - ReDoc",
        redoc_js_url="/static/redoc.standalone.js",
    )


@app.post("/register", response_model=BaseResponse)
def register_user(user: UserCreate, db: Session = Depends(get_db)):
    if not user.is_valid_password:
        raise HTTPException(status_code=422, detail="Password must be at least 8 characters long and contain at least one uppercase letter.")

    if user.password != user.verify_password:
        raise HTTPException(status_code=400, detail="Passwords do not match.")

    existing_user = db.query(User).filter(User.email == user.email).first()
    existing_pending_user = db.query(PendingUser).filter(PendingUser.email == user.email).first()
    
    if existing_user:
        raise HTTPException(status_code=400, detail="Email already registered.")
    if existing_pending_user:
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
            user_id=pending_user.id,
            username=pending_user.username,
            registration_request_description=pending_user.registration_request_description
        )
        for pending_user in pending_users
    ]

@app.post("/registration-requests/{pending_user_id}/approve-registration/")
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


@app.delete("/registration-requests/{pending_user_id}/deny-registration/")
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


@app.post("/promote_user/{username}", response_model=dict)
def promote_user_to_manager(
    username: str,
    db: Session = Depends(get_db),
    current_user: User = Depends(verify_manager)
):
    if current_user.role != Role.manager:
        raise HTTPException(
            status_code=status.HTTP_403_FORBIDDEN,
            detail="Only managers can promote users"
        )
    
    user_to_promote = db.query(User).filter(User.username == username).first()
    
    if not user_to_promote:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail="User not found"
        )
    
    if user_to_promote.role == Role.manager:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="User is already a manager"
        )
    
    user_to_promote.role = Role.manager
    db.commit()
    db.refresh(user_to_promote)

    return {"message": f"User '{username}' promoted to manager", "user_id": user_to_promote.id}

@app.post("/downgrade_user/{username}", response_model=dict)
def downgrade_user_to_viewonly(
    username: str,
    db: Session = Depends(get_db),
    current_user: User = Depends(verify_manager)
):
    if current_user.role != Role.manager:
        raise HTTPException(
            status_code=status.HTTP_403_FORBIDDEN,
            detail="Only managers can downgrade users"
        )
    
    user_to_downgrade = db.query(User).filter(User.username == username).first()
    
    if not user_to_downgrade:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail="User not found"
        )
    
    if user_to_downgrade.role == Role.viewonly:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="User is already viewonly"
        )
    
    user_to_downgrade.role = Role.viewonly
    db.commit()
    db.refresh(user_to_downgrade)

    return {"message": f"User '{username}' downgraded to viewonly", "user_id": user_to_downgrade.id}

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

