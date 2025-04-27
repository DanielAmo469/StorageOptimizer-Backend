from typing import Literal, Optional, List, Dict, Any
from pydantic import BaseModel, EmailStr, Field
from datetime import datetime

class UserCreate(BaseModel):
    username: str
    email: EmailStr
    password: str 
    verify_password: str
    registration_request_description: str

    # Enforce custom validation for password
    @property
    def is_valid_password(self):
        if len(self.password) < 8:
            return False
        if not any(c.isupper() for c in self.password):
            return False
        return True
    

class BaseResponse(BaseModel):
    message: str 
    user_id: int  


class UserValues(BaseModel):
    user_id: int
    username: str
    email: str
    date_created: datetime
    
class RegistrationRequests(BaseModel):
    user_id: int
    username: str
    registration_request_description:str


class FileInfo(BaseModel):
    full_path: str
    creation_time: str
    last_access_time: str
    last_modified_time: str
    file_size: int


class RestoreRequest(BaseModel):
    archived_path: str

class DateRange(BaseModel):
    start_date: Optional[str] = None
    end_date: Optional[str] = None


class DateFilters(BaseModel):
    creation_time: Optional[DateRange] = None
    last_access_time: Optional[DateRange] = None
    last_modified_time: Optional[DateRange] = None


class ArchiveFilterRequest(BaseModel):
    share_name: Literal["data1", "data2"] = Field(..., description="The share to archive from (data1 or data2 only)")
    file_type: Optional[List[str]] = None
    date_filters: Optional[DateFilters] = None
    min_size: Optional[int] = None
    max_size: Optional[int] = None
    blacklist: Optional[List[str]] = []

class BlacklistUpdate(BaseModel):
    blacklist: list[str]