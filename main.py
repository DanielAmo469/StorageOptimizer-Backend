from io import BytesIO
import json
from typing import Any, Dict, List, Optional
from datetime import timedelta
from fastapi import Body, FastAPI, Depends, HTTPException, Request, status
from fastapi.responses import StreamingResponse
from sqlalchemy.orm import Session
from passlib.hash import bcrypt
from jose import JWTError, jwt
from fastapi.security import OAuth2PasswordRequestForm
from fastapi.middleware.cors import CORSMiddleware
from fastapi.staticfiles import StaticFiles
from fastapi.openapi.docs import get_swagger_ui_html
from apscheduler.schedulers.background import BackgroundScheduler


from database import SessionLocal, engine, Base, get_db
from models import PendingUser, Role, User
from netapp_btc import filter_files, get_svm_data_volumes, scan_volume
from netapp_interfaces import analyze_volume_for_archive_and_restore, archive_filtered_files, bulk_restore_files, move_file, bulk_move_files, restore_file, restore_multiple_files
from scan_manager import scan_all_volumes_and_process
from schemas import ArchiveFilterRequest, ArchiveRestoreRequest, BaseResponse, BlacklistUpdate, FileInfo, RegistrationRequests, RestoreRequest, UserCreate, UserValues
from services import SETTINGS_FILE, build_filters_from_request, get_dynamic_settings_model, get_user_id_by_username, verify_manager
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

scheduler = BackgroundScheduler()
DynamicSettingsModel = get_dynamic_settings_model()


def scheduled_volume_scan():
    print("[SCHEDULED SCAN] Running scan_all_volumes_and_process...")
    scan_all_volumes_and_process()  # You need to implement or import this function

# Add this in app startup:
@app.on_event("startup")
def start_scheduler():
    scheduler.add_job(scheduled_volume_scan, 'interval', days=1)
    scheduler.start()

@app.get("/docs", include_in_schema=False)
async def custom_swagger_ui_html():
    return get_swagger_ui_html(
        openapi_url="/openapi.json",
        title="Storage Optimizer - Swagger UI",
        swagger_js_url="/static/swagger-ui-bundle.js",
        swagger_css_url="/static/swagger-ui.css",       
    )

@app.get("/openapi.json", include_in_schema=False)
async def custom_openapi():
    return JSONResponse( # type: ignore
        get_openapi( # type: ignore
            title="Storage Optimizer",
            version="1.0.0",
            routes=app.routes
        )
    )


@app.get(app.swagger_ui_oauth2_redirect_url, include_in_schema=False)
async def swagger_ui_redirect():
    return get_swagger_ui_oauth2_redirect_html() # type: ignore


@app.get("/redoc", include_in_schema=False)
async def redoc_html():
    return get_redoc_html( # type: ignore
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

@app.post("/manual-scan")
def manual_scan(current_user: User = Depends(verify_manager)):
    try:
        scan_all_volumes_and_process()
        return {"status": "success", "message": "Manual scan executed successfully."}
    except Exception as e:
        return {"status": "error", "reason": str(e)}


@app.post("/archive-multiple", response_model=dict)
def archive_multiple_files_endpoint(
    files: List[FileInfo],
    current_user: User = Depends(verify_manager)
):
    try:
        print(f"Starting bulk archive of {len(files)} files...")

        # Convert FileInfo (Pydantic models) to plain dicts
        file_dicts = [f.dict() for f in files]

        moved, failed = bulk_move_files(file_dicts)

        return {
            "status": "complete",
            "archive_summary": {
                "success_count": len(moved),
                "failed_count": len(failed),
                "failures": failed
            }
        }

    except Exception as e:
        print(f"Error during archive-multiple: {e}")
        return {
            "status": "error",
            "reason": str(e)
        }

@app.post("/restore-multiple", response_model=dict)
def restore_multiple_files_endpoint(
    restore_requests: List[RestoreRequest],
    current_user: User = Depends(verify_manager)
):
    try:
        print(f"Starting restore process for multiple files...")

        # Convert Pydantic objects to dicts
        restore_dicts = [r.dict() for r in restore_requests]

        result = restore_multiple_files(restore_dicts)

        return result

    except ValueError as ve:
        print(f"ValueError during restore: {ve}")
        return {"status": "error", "reason": str(ve)}
    except Exception as e:
        print(f"Unexpected error during restore: {e}")
        return {"status": "error", "reason": f"Failed to restore files: {str(e)}"}


@app.get("/admin/get-settings")
def get_settings(current_user: User = Depends(verify_manager)):
    try:
        with open(SETTINGS_FILE, "r") as f:
            settings = json.load(f)
        return {"status": "success", "settings": settings}
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Failed to load settings: {e}")

@app.post("/admin/update-settings")
def update_settings(
    updated_values: dict = Body(...),
    current_user: User = Depends(verify_manager)
):
    with open("settings.json", "r") as f:
        settings = json.load(f)

    # Only update existing keys (top-level)
    for key, value in updated_values.items():
        if key in settings:
            settings[key] = value
        else:
            raise HTTPException(status_code=400, detail=f"Invalid key: {key}")

    with open("settings.json", "w") as f:
        json.dump(settings, f, indent=4)

    return {"status": "success", "settings": settings}


@app.post("/preview-filtered-files", response_model=dict)
def preview_filtered_files(
    filter_request: ArchiveFilterRequest,
    current_user: User = Depends(verify_manager)
):
    filters = {
        "file_type": filter_request.file_type or [],
        "date_filters": filter_request.date_filters.dict() if filter_request.date_filters else {},
        "min_size": filter_request.min_size,
        "max_size": filter_request.max_size,
    }

    try:
        result = analyze_volume_for_archive_and_restore(
            share_name=filter_request.share_name,
            filters=filters,
            blacklist=filter_request.blacklist or []
        )

        archive_candidates = result.get("archive_candidates", [])
        restore_candidates = result.get("restore_candidates", [])

        return {
            "status": "success",
            "archive_candidates_count": len(archive_candidates),
            "restore_candidates_count": len(restore_candidates),
            "archive_candidates": archive_candidates,
            "restore_candidates": restore_candidates
        }

    except Exception as e:
        print(f"Error during preview-filtered-files: {e}")
        return {"status": "error", "reason": str(e)}

@app.post("/execute-filtered-transfer", response_model=dict)
def execute_filtered_transfer(
    request: ArchiveFilterRequest,
    current_user: User = Depends(verify_manager)
):
    filters = {
        "file_type": request.file_type or [],
        "date_filters": request.date_filters.dict() if request.date_filters else {},
        "min_size": request.min_size,
        "max_size": request.max_size,
    }

    blacklist = request.blacklist or []
    share_name = request.share_name

    db = SessionLocal()
    try:
        #Analyze share and determine archive/restore candidates
        result = analyze_volume_for_archive_and_restore(share_name, filters, blacklist)
        archive_candidates = result.get("archive_candidates", [])
        restore_candidates = result.get("restore_candidates", [])

        valid_archive_candidates = [f for f in archive_candidates if f.get("full_path")]

        #Archive files
        archive_result = {
            "moved_files": [],
            "failed_files": []
        }

        if valid_archive_candidates:
            print(f"Archiving {len(valid_archive_candidates)} files...")
            moved, failed = bulk_move_files(valid_archive_candidates)
            archive_result["moved_files"] = moved
            archive_result["failed_files"] = failed
        else:
            print("No valid archive candidates.")

        #Restore files
        restore_result = {
            "restored": [],
            "skipped": []
        }

        if restore_candidates:
            print(f"Restoring {len(restore_candidates)} files...")
            for file in restore_candidates:
                print(f"→ restore: {file.get('archived_path') or file.get('full_path') or 'UNKNOWN'}")
            restore_result = bulk_restore_files(restore_candidates, db)

        return {
            "status": "complete",
            "archive_summary": {
                "success_count": len(archive_result.get("moved_files", [])),
                "failed_count": len(archive_result.get("failed_files", [])),
                "failures": archive_result.get("failed_files", [])
            },
            "restore_summary": {
                "restored_count": len(restore_result.get("restored", [])),
                "skipped_count": len(restore_result.get("skipped", [])),
                "skipped": restore_result.get("skipped", [])
            }
        }

    except Exception as e:
        print(f"[ERROR] execute-filtered-transfer failed: {e}")
        return {"status": "error", "reason": str(e)}
    finally:
        db.close()


# Archive filtered files endpoint
@app.post("/archive-filtered-files", response_model=dict)
def archive_filtered_files_endpoint(
    filter_request: ArchiveFilterRequest,
    current_user: User = Depends(verify_manager)
):
    try:
        print(f"Starting archive process for filtered files on share: {filter_request.share_name}")

        filters = build_filters_from_request(filter_request)

        volume_data = get_svm_data_volumes()
        volumes = volume_data.get("volumes", [])
        volume_info = next((v for v in volumes if v.get('share_name') == filter_request.share_name), None)

        if not volume_info:
            raise ValueError(f"Volume info for share '{filter_request.share_name}' not found.")

        all_files = scan_volume(
            share_name=filter_request.share_name,
            volume=volume_info,
            blacklist=filter_request.blacklist or []
        )

        if not all_files:
            return {"status": "no_files", "reason": f"No files found in {filter_request.share_name}"}

        scanned_dict = {filter_request.share_name: all_files}

        filtered = filter_files(
            files=scanned_dict,
            filters=filters,
            blacklist=filter_request.blacklist or [],
            share_name=filter_request.share_name
        )

        matching_files = filtered.get(filter_request.share_name, [])
        

        if not matching_files:
            return {"status": "no_matches", "reason": "No files matched filters"}

        print(f"Found {len(matching_files)} matching files after filter.")

        result = archive_filtered_files(
            cold_files=matching_files,
            share_name=filter_request.share_name,
            volume=volume_info,
            blacklist=filter_request.blacklist or []
        )

        return result

    except ValueError as ve:
        print(f"ValueError during archive: {ve}")
        return {"status": "error", "reason": str(ve)}
    except Exception as e:
        print(f"Unexpected error during archive: {e}")
        return {"status": "error", "reason": f"Failed to archive files: {str(e)}"}


app.add_middleware(
    CORSMiddleware,
    allow_origins=["http://localhost:3000"],  # React app running on port 3000
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

