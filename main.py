from datetime import timedelta
from fastapi import FastAPI, Query
from sqlalchemy import bindparam, create_engine, text
from sqlalchemy.orm import sessionmaker
from typing import Optional, List
from fastapi.responses import JSONResponse
from fastapi.middleware.cors import CORSMiddleware
from prophet import Prophet

app = FastAPI()

app.add_middleware(
    CORSMiddleware,
    allow_origins=["http://localhost:9000"], 
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

DATABASE_URL = "postgresql+psycopg2://postgres:67206720Ks%40%40@127.0.0.1:5432/datacube"
engine = create_engine(DATABASE_URL)
SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine)

app = FastAPI()

app.add_middleware(
    CORSMiddleware,
    allow_origins=["http://localhost:9000"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

DATABASE_URL = "postgresql+psycopg2://postgres:67206720Ks%40%40@127.0.0.1:5432/datacube"
engine = create_engine(DATABASE_URL)
SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine)

@app.get("/air-quality")
def read_air_quality(
    region: Optional[str] = Query(None),
    date: Optional[str] = Query(None),
    pollutant: Optional[List[str]] = Query(None)  # Accepts multiple
):
    session = SessionLocal()
    try:
        filters = []
        params = {}

        if region:
            filters.append("region = :region")
            params["region"] = region

        if date:
            filters.append("date LIKE :date")
            params["date"] = f"{date}%"

        if pollutant:
            filters.append("pollutant IN :pollutants")
            params["pollutants"] = tuple(pollutant)  

        where_clause = f"WHERE {' AND '.join(filters)}" if filters else ""

        query = text(f"""
            SELECT jsonb_build_object(
                'type',       'Feature',
                'geometry',   ST_AsGeoJSON(geom)::jsonb,
                'properties', jsonb_build_object(
                    'region', region,
                    'date', date,
                    'pollutant', pollutant,
                    'concentration', concentration
                )
            ) AS feature
            FROM odc_data
            {where_clause}
        """)

        # Make sure to bind expanding parameter for pollutant list
        if "pollutants" in params:
            query = query.bindparams(bindparam("pollutants", expanding=True))

        result = session.execute(query, params)
        features = [row[0] for row in result]
        geojson = {
            "type": "FeatureCollection",
            "features": features
        }
        return JSONResponse(content=geojson)
    finally:
        session.close()

@app.get("/regions")
def read_regions():
    session = SessionLocal()
    try:
        query = text("""
            SELECT DISTINCT region
            FROM odc_data
            ORDER BY region
            LIMIT 52
        """)
        result = session.execute(query)
        regions = [row[0] for row in result]
        return {"regions": regions}
    finally:
        session.close()

@app.get("/pollutants")
def read_pollutants():
    session = SessionLocal()
    try:
        query = text("""
            SELECT DISTINCT pollutant
            FROM odc_data
            ORDER BY pollutant
            LIMIT 5
        """)
        result = session.execute(query)
        pollutants = [row[0] for row in result]
        return {"pollutants": pollutants}
    finally:
        session.close()



