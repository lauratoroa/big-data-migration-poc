from fastapi import FastAPI
from backups.bk_routes import router as backup_router
app = FastAPI()

# Include routes for backups
app.include_router(backup_router)

@app.get("/")
def health_check():
    return {"message": "Big Data Migration API is running"}
