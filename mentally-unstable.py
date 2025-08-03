from sqlalchemy import create_engine, text
import pandas as pd
from prophet import Prophet
import matplotlib.pyplot as plt

DATABASE_URL = "postgresql+psycopg2://postgres:67206720Ks%40%40@127.0.0.1:5432/datacube"
engine = create_engine(DATABASE_URL)