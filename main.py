from datetime import timedelta
import logging
from fastapi import FastAPI, Query
from sqlalchemy import bindparam, create_engine, text
from sqlalchemy.orm import sessionmaker
from typing import Optional, List
from fastapi.responses import JSONResponse
from fastapi.middleware.cors import CORSMiddleware
from prophet import Prophet
from fastapi import HTTPException
import pandas as pd
from datetime import datetime
import xgboost as xgb
from pandas.tseries.frequencies import to_offset

from fastapi.responses import JSONResponse

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

OZONE = "Ozone (O₃) µg/m³"
DAILY_POLLUTANTS = [
    "Particulate Matter 10 µg/m³",
    "Particulate Matter 2.5 µg/m³",
    "Sulphur Dioxide (SO₂) µg/m³",
    "Nitrogen Dioxide (NO₂) µg/m³"
]

app = FastAPI()

@app.get("/forecast")
def forecast_air_quality(
    region: str = Query(...),
    date: str = Query(...),
    pollutant: Optional[List[str]] = Query(None)
):
    print(f"Received forecast request for region={region}, date={date}, pollutant={pollutant}")

    try:
        target_date = datetime.strptime(date, "%Y-%m-%d").date()
        print(f"Parsed target_date: {target_date}")
    except Exception as e:
        print(f"Date parsing failed: {e}")
        logging.exception("Forecasting failed due to invalid date format")
        raise HTTPException(status_code=400, detail="Invalid date format, expected YYYY-MM-DD")

    session = SessionLocal()
    try:
        if not pollutant:
            print("No pollutant specified, fetching all pollutants for region")
            pollutant_query = text("SELECT DISTINCT pollutant FROM odc_data WHERE region = :region")
            result = session.execute(pollutant_query, {"region": region})
            pollutant = [row[0] for row in result]
            print(f"Pollutants found: {pollutant}")

        output = []

        for pol in pollutant:
            print(f"\n--- Processing pollutant: {pol} ---")

            if pol == OZONE:
                query = text("""
                    SELECT date, concentration
                    FROM odc_data
                    WHERE region = :region AND pollutant = :pollutant
                    ORDER BY date
                """)
                data = session.execute(query, {"region": region, "pollutant": pol}).fetchall()
                print(f"Fetched {len(data)} records for ozone")

                if not data:
                    print(f"No data found for pollutant {pol}, skipping")
                    continue

                df = pd.DataFrame(data, columns=["ds", "y"])
                print(f"Data sample (ozone):\n{df.head()}")
                df["ds"] = pd.to_datetime(df["ds"])
                df = df.sort_values("ds").reset_index(drop=True)

                # Create lag features for t-1, t-2, t-3
                df["ozone_t-1"] = df["y"].shift(1)
                df["ozone_t-2"] = df["y"].shift(2)
                df["ozone_t-3"] = df["y"].shift(3)

                # Time features
                df["hour"] = df["ds"].dt.hour
                df["day_of_week"] = df["ds"].dt.dayofweek
                df["day_of_year"] = df["ds"].dt.dayofyear

                df = df.dropna()
                print(f"Data after creating lag and time features, rows count: {len(df)}")
                print(f"Data sample after feature engineering:\n{df.head()}")

                features = ["ozone_t-1", "ozone_t-2", "ozone_t-3", "hour", "day_of_week", "day_of_year"]
                target = "y"

                X = df[features]
                y = df[target]

                # Train XGBoost model
                model = xgb.XGBRegressor(n_estimators=100, max_depth=5, learning_rate=0.1)
                model.fit(X, y)
                print("XGBoost model trained for ozone")

                # Fixed forecast horizon of 3 (3 intervals per day: 00:00, 08:00, 16:00)
                forecast_horizon = 3
                print(f"Forecast horizon (8h intervals): {forecast_horizon}")

                predictions = []
                last_known = df.iloc[-1].copy()
                print(f"Last known record used for lag features:\n{last_known}")

                base_date = pd.to_datetime(date)
                lag_values = {
                    "ozone_t-1": last_known["y"],
                    "ozone_t-2": last_known["ozone_t-1"],
                    "ozone_t-3": last_known["ozone_t-2"],
                }
                print(f"Initial lag values: {lag_values}")

                for i, hour in enumerate([0, 8, 16]):
                    pred_time = base_date + pd.Timedelta(hours=hour)
                    print(f"Predicting for time: {pred_time}")

                    input_features = {
                        "ozone_t-1": lag_values["ozone_t-1"],
                        "ozone_t-2": lag_values["ozone_t-2"],
                        "ozone_t-3": lag_values["ozone_t-3"],
                        "hour": pred_time.hour,
                        "day_of_week": pred_time.dayofweek,
                        "day_of_year": pred_time.dayofyear,
                    }
                    input_df = pd.DataFrame([input_features])
                    pred_conc = model.predict(input_df)[0]
                    pred_conc = float(max(pred_conc, 0))
                    print(f"Prediction at {pred_time}: {pred_conc}")

                    predictions.append({
                        "region": region,
                        "date": pred_time.strftime("%Y-%m-%d %H:%M:%S"),
                        "pollutant": pol,
                        "concentration": pred_conc,
                    })

                    # Update lag values for next prediction
                    lag_values["ozone_t-3"] = lag_values["ozone_t-2"]
                    lag_values["ozone_t-2"] = lag_values["ozone_t-1"]
                    lag_values["ozone_t-1"] = pred_conc
                    print(f"Updated lag values: {lag_values}")

                output.extend(predictions)

            elif pol in DAILY_POLLUTANTS:
                query = text("""
                    SELECT date, concentration
                    FROM odc_data
                    WHERE region = :region AND pollutant = :pollutant
                    ORDER BY date
                """)
                data = session.execute(query, {"region": region, "pollutant": pol}).fetchall()
                print(f"Fetched {len(data)} records for daily pollutant {pol}")

                df = pd.DataFrame(data, columns=["ds", "y"])
                df["ds"] = pd.to_datetime(df["ds"]).dt.date
                df = df.groupby("ds").mean().reset_index()
                df["ds"] = pd.to_datetime(df["ds"])

                if df.empty:
                    print(f"No data found for pollutant {pol}, skipping")
                    continue

                print("Last 5 rows of training data:")
                print(df.tail(5))

                max_existing_date = df["ds"].max().date()
                print(f"Max existing date in data: {max_existing_date}")

                forecast_horizon = (target_date - max_existing_date).days
                if forecast_horizon <= 0:
                    forecast_horizon = 1
                print(f"Forecast horizon (days): {forecast_horizon}")

                model = Prophet()
                model.fit(df)
                print("Prophet model fitted")

                future = model.make_future_dataframe(periods=forecast_horizon)
                forecast = model.predict(future)
                print("Forecast generated")

                pred_row = forecast[forecast["ds"].dt.date == target_date]
                if not pred_row.empty:
                    concentration = max(pred_row.iloc[0]["yhat"], 0)
                    print(f"Predicted concentration for {pol} on {target_date}: {concentration}")
                    output.append({
                        "region": region,
                        "date": target_date.strftime("%Y-%m-%d"),
                        "pollutant": pol,
                        "concentration": concentration
                    })
                else:
                    print(f"No forecast data available for {pol} on {target_date}")

            else:
                print(f"Pollutant {pol} is not recognized for forecasting, skipping")

        print(f"\nReturning forecast output with {len(output)} records")
        return JSONResponse(content=output)

    finally:
        session.close()
        print("Database session closed")







