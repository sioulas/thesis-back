from fastapi.middleware.cors import CORSMiddleware
from fastapi import FastAPI

app = FastAPI()
app.add_middleware(
    CORSMiddleware,
    allow_origins=["http://localhost:9000"],  # Quasar dev server
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

