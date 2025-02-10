import os
from fastapi import FastAPI, HTTPException, Depends
from sqlalchemy import create_engine, Column, Integer, String, Float
from sqlalchemy.orm import sessionmaker, declarative_base, Session
import time
from pydantic import BaseModel
from datetime import datetime
import datetime as dt
from dotenv import load_dotenv

# Load environment variables from .env file
load_dotenv()

# Database Connection
DATABASE_URL = os.getenv("AZURE_SQL_CONNECTION_STRING")
engine = create_engine(DATABASE_URL)
SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine)
Base = declarative_base()


# Database Models
class User(Base):
    __tablename__ = "users"
    id = Column(Integer, primary_key=True, index=True)
    name = Column(String, nullable=False)
    email = Column(String, unique=True, nullable=False)

class AuditLog(Base):
    __tablename__ = "audit_log"
    id = Column(Integer, primary_key=True, index=True)
    query = Column(String, nullable=False)
    execution_time = Column(Float, nullable=False)
    status = Column(String, nullable=False)

class UserCreate(BaseModel):
    name: str
    email: str

class RCARequest(BaseModel):
    start_time: datetime
    end_time: datetime


# Dependency to get DB session
def get_db():
    db = SessionLocal()
    try:
        yield db
    finally:
        db.close()


app = FastAPI()

# Insert user API
@app.post("/users")
def create_user(user: UserCreate, db: Session = Depends(get_db)):
    start_time = time.time()
    try:
        new_user = User(name=user.name, email=user.email)
        db.add(new_user)
        db.commit()
        db.refresh(new_user)
        execution_time = time.time() - start_time

        audit_log = AuditLog(query=f"INSERT INTO users (name, email) VALUES ('{user.name}', '{user.email}')",
                             execution_time=execution_time,
                             status="success")
        db.add(audit_log)
        db.commit()
        return new_user
    except Exception as e:
        db.rollback()
        execution_time = time.time() - start_time
        audit_log = AuditLog(query="INSERT INTO users", execution_time=execution_time, status="failure", error=str(e))
        db.add(audit_log)
        db.commit()
        raise HTTPException(status_code=400, detail="Error inserting user")

# Get user list API
@app.get("/users")
def get_users(db: Session = Depends(get_db)):
    try:
        users = db.query(User).all()
        return users
    except Exception as e:
        raise HTTPException(status_code=500, detail="Error fetching users")
