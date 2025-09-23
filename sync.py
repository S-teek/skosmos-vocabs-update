import os
import asyncio
import logging
from fastapi import FastAPI, Request, HTTPException
from fastapi.responses import JSONResponse
import requests
import uvicorn


app = FastAPI()

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger()

API_KEY = os.getenv("VOCABS_UPDATE_API_KEY")

SYNC_INTERVAL = int(os.getenv("SYNC_INTERVAL", "86400"))

sync_lock = asyncio.Lock()

async def do_sync():
    sources = [
        "https://raw.githubusercontent.com/LTER-Europe/SO/refs/heads/main/standard-observations.ttl",
        "https://raw.githubusercontent.com/LTER-Europe/EnvThes/refs/heads/main/EnvThes.ttl",
        "https://raw.githubusercontent.com/LTER-Europe/eLTER_CL/refs/heads/main/elter_cl.ttl"
    ]

    url = "http://localhost:3030/skosmos/data"
    params = {
        "graph": "http://skos.um.es/unescothes/"
    }
    headers = {
        "Content-Type": "text/turtle"
    }
    logger.info(">>> Sync started")
    await asyncio.sleep(1)

    files = []
    for source in sources:
        response = requests.get(source, allow_redirects=True)
        file = source.split("/")[-1]
        files.append(file)
        open(file, "wb").write(response.content)

    for file in [files]:
        with open(file, "rb") as f:
            logger.info(">>> Uploading", file)
            response = requests.post(url, params=params, headers=headers, data=f)
            logger.info(">>> ", response.status_code, response.text)
    logger.info(">>> Sync finished")

async def periodic_sync():
    while True:
        async with sync_lock:
            await do_sync()
        await asyncio.sleep(SYNC_INTERVAL)

@app.on_event("startup")
async def startup_event():
    asyncio.create_task(periodic_sync())

@app.post("/sync")
async def manual_sync(request: Request):
    """Manual trigger endpoint protected by API key."""
    auth_header = request.headers.get("Authorization", "")
    if auth_header != f"Bearer {API_KEY}":
        raise HTTPException(status_code=401, detail="Unauthorized")

    async def trigger():
        async with sync_lock:
            await do_sync()
    asyncio.create_task(trigger())

    return JSONResponse({"status": "manual sync triggered"})

if __name__ == "__main__":
    uvicorn.run(app, host="0.0.0.0", port=8080)
