# app.py
from fastapi import FastAPI
from sqlalchemy import create_engine, text
from sqlalchemy.orm import sessionmaker
from typing import List
import os

app = FastAPI()

# Database connection (update with your actual credentials)
DATABASE_URL = "postgresql+psycopg2://postgres:67206720Ks%40%40@127.0.0.1:5432/datacube"
engine = create_engine(DATABASE_URL)
SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine)

@app.get("/air-quality")
def read_air_quality():
    session = SessionLocal()
    try:
        result = session.execute(text("SELECT * FROM air_quality"))
        rows = [dict(row._mapping) for row in result]
        return rows
    finally:
        session.close()
