import os
from contextlib import asynccontextmanager
import logging
import time
from fastapi import FastAPI, Request, HTTPException
from fastapi.responses import JSONResponse
import requests
import uvicorn
import threading

@asynccontextmanager
async def lifespan(app: FastAPI):
    thread = threading.Thread(target=periodic_sync, daemon=True)
    thread.start()
    yield

app = FastAPI(lifespan=lifespan)

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger()

API_KEY = os.getenv("VOCABS_UPDATE_API_KEY")
USER = os.getenv("FUSEKI_USER")
PASSWORD = os.getenv("FUSEKI_PASSWORD")

SYNC_INTERVAL = int(os.getenv("SYNC_INTERVAL", "10"))

sync_lock = threading.Lock()

def sync_safe():
    with sync_lock:
        return sync()


def sync():
    sources = [
        "https://raw.githubusercontent.com/LTER-Europe/SO/refs/heads/main/standard-observations.ttl",
        "https://raw.githubusercontent.com/LTER-Europe/EnvThes/refs/heads/main/EnvThes.ttl",
        "https://raw.githubusercontent.com/LTER-Europe/eLTER_CL/refs/heads/main/elter_cl.ttl"
    ]

    graph_uris = [
        "http://skos.um.es/unescothes/",
        "http://vocabs.lter-europe.net/EnvThes/",
        "http://vocabs.lter-europe.net/elter_cl/"
    ]

    url = "https://vocabs.elter-ri.eu/fuseki/skosmos/data"
    
    headers = {
        "Content-Type": "text/turtle"
    }
    logger.info(">>> Sync started")

    files = []
    for source in sources:
        response = requests.get(source, allow_redirects=True)
        file = source.split("/")[-1]
        files.append(file)
        open(file, "wb").write(response.content)

    for i, file in enumerate(files):
        with open(file, "rb") as bin_f:
            data=bin_f.read()
            logger.info(f">>> Uploading {file}")
            response = requests.post(f"{url}?graph={graph_uris[i]}", headers=headers, data=data, auth=requests.auth.HTTPBasicAuth(USER, PASSWORD))
            logger.info(f">>> {response.status_code}, {response.text} from uploading {file}")
    logger.info(">>> Sync finished")
    return "Updated"


def periodic_sync():
    while True:
        try:
            sync_safe()
        except Exception as e:
            print("Error during sync:", e)
        time.sleep(SYNC_INTERVAL * 60)


@app.post("/sync")
def manual_sync(request: Request):
    """Manual trigger endpoint protected by API key."""
    logger.info("Request to /sync")
    auth_header = request.headers.get("Authorization", "")
    if auth_header != f"Bearer {API_KEY}":
        raise HTTPException(status_code=401, detail="Unauthorized")

    result = sync_safe()

    return JSONResponse({"status": result})

if __name__ == "__main__":
    uvicorn.run("sync:app", host="0.0.0.0", port=8000, reload=False)

