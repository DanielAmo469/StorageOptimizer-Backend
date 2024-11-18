from typing import Optional, List, Dict, Any
from pydantic import BaseModel, EmailStr
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
    username: str
    registration_request_description:str