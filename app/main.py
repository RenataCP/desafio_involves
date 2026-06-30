import os
import json
import logging
import psycopg2
import psycopg2.extras

from fastapi import FastAPI, HTTPException

class JsonFormarter(logging.Formatter):
    def format(self, record):
        return json.dumps({
            "timestamp": self.formatTime(record),
            "level": record.levelname,
            "service": "pokedex_api",
            "message": record.getMessage(),
        })

handler = logging.StreamHandler()
handler.setFormatter(JsonFormarter())
logger = logging.getLogger("pokedex_api")
logging.getLogger().addHandler(handler)
logging.getLogger().setLevel(logging.INFO)

app = FastAPI(
    title="Pokedex API",
    description="Pokemon data served from the Pokemon ETL",
    version="1.0.0"
)

def get_connection():
    return psycopg2.connect(
        host=os.environ["DB_HOST"],
        port=int(os.environ.get("DB_PORT", 5432)),
        dbname=os.environ["DB_NAME"],
        user=os.environ["DB_USER"],
        password=os.environ["DB_PASSWORD"]
    )

@app.get("/health")
def health():
    try:
        conn = get_connection()
        conn.close()
        return {"status": "ok"}
    except Exception as e:
        logger.error("Health check failed", extra={"error": str(e)})
        raise HTTPException(status_code=503, detail="database unavailable")

@app.get("/pokemon")
def list_pokemon():
    conn = get_connection()
    try:
        with conn.cursor() as cursor:
            cursor.execute("SELECT id, name FROM curated.pokemon ORDER BY id")
            rows = cursor.fetchall()

            return [{"id": row[0], "name": row[1]} for row in rows]
    finally:
        conn.close()

@app.get("/pokemon/{pokemon_id}")
def get_pokemon(pokemon_id: int):
    conn = get_connection()
    try:
        with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cursor:
            cursor.execute("SELECT * FROM curated.pokemon WHERE id = %s", (pokemon_id,))
            row = cursor.fetchone()
        if not row:
            raise HTTPException(status_code=404, detail="Pokemon not found")
        return dict(row)
    finally:
        conn.close()