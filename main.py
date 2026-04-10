from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from app.api import auth, users, surveys, news, settings_api, locations, contact, metrics, map_api

app = FastAPI(title="PulpeyBox Admin API", version="1.0.0", docs_url="/api/docs")

app.add_middleware(
    CORSMiddleware,
    allow_origins=["http://localhost:5173", "http://localhost:3000", "http://localhost"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

app.include_router(auth.router, prefix="/api")
app.include_router(users.router, prefix="/api")
app.include_router(surveys.router, prefix="/api")
app.include_router(news.router, prefix="/api")
app.include_router(settings_api.router, prefix="/api")
app.include_router(locations.router, prefix="/api")
app.include_router(contact.router, prefix="/api")
app.include_router(metrics.router, prefix="/api")
app.include_router(map_api.router, prefix="/api")


@app.get("/api/health")
def health():
    return {"status": "ok"}
