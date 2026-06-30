# # #
# Pokemon ETL DAG
# Extracts from PokeAPI, stages in raw.pokeapi, loads to curated.pokemon
# # #

import json
import logging
import requests

from typing import Any
from datetime import datetime

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook

dag_name = "pokemon_etl"

class JsonFormarter(logging.Formatter):
    def format(self, record):
        return json.dumps({
            "timestamp": self.formatTime(record),
            "level": record.levelname,
            "service": dag_name,
            "message": record.getMessage(),
        })

handler = logging.StreamHandler()
handler.setFormatter(JsonFormarter())
logger = logging.getLogger(dag_name)
logger.addHandler(handler)
logger.setLevel(logging.INFO)

def _get_connection():
    return PostgresHook(postgres_conn_id="postgres_data")

# # #
# Setup Task
# # #

def create_tables(**context: Any) -> None:
    try:
        conn = _get_connection()

        conn.run("CREATE SCHEMA IF NOT EXISTS raw;")
                            
        conn.run("""CREATE TABLE IF NOT EXISTS raw.pokeapi (
                        id INTEGER,
                        name VARCHAR,
                        payload JSONB,
                        ingested_at TIMESTAMP DEFAULT NOW(),
                        PRIMARY KEY (id, ingested_at)    
                    );
        """)
                               
        conn.run("CREATE SCHEMA IF NOT EXISTS curated;")
                               
        conn.run("""CREATE TABLE IF NOT EXISTS curated.pokemon (
                        id INTEGER PRIMARY KEY,
                        name VARCHAR,
                        height_m NUMERIC(5,2),
                        weight_kg NUMERIC(7,2),
                        types VARCHAR,
                        hp INTEGER,
                        attack INTEGER,
                        defense INTEGER,
                        special_attack INTEGER,
                        special_defense INTEGER,
                        speed INTEGER,
                        updated_at TIMESTAMP DEFAULT NOW()
                    );
        """)
    except Exception as e:
        logger.info(f"Fail to create Schema or Table: {e}")
        raise

def set_app_permissions():
    try:
        conn = _get_connection()

        conn.run("GRANT USAGE ON SCHEMA curated TO pokedex_api")
        conn.run("GRANT SELECT ON curated.pokemon TO pokedex_api")
        
    except Exception as e:
        logger.error(e)

# # #
# Extraction Task
# # # 

POKEAPI_BASE_URL = "https://pokeapi.co/api/v2"

def fetch_pokemon_count(**context: Any) -> int:
    url = f"{POKEAPI_BASE_URL}/pokemon?limit=1"
    response = requests.get(url)
    response.raise_for_status()
    pokemon_count = response.json()["count"]
    logger.info(f"Total pokemon count: {pokemon_count}")
    return pokemon_count

def extract(**context: Any) -> dict[str, int]:

    pokemon_count = fetch_pokemon_count()
    # logger.info(f"Found {pokemon_count}, but will use just 10 pokemons")
    # pokemon_count = 1000

    success = 0
    failed = 0

    conn = _get_connection()

    for pokemon_id in range(1, pokemon_count+1):
        url = f"{POKEAPI_BASE_URL}/pokemon/{pokemon_id}"
        try:
            response = requests.get(url)
            response.raise_for_status()
            data = response.json()

            conn.run(
                "INSERT INTO raw.pokeapi (id, name, payload) VALUES (%(id)s, %(name)s, %(payload)s)",
                parameters={"id":data["id"],"name": data["name"],"payload": json.dumps(data)},
            )
            success += 1
        except Exception as e:
            failed += 1
            logger.error(f"Failed to extract {data["name"]}: {e}")

    logger.info(f"Extraction complete - Success: {success} | Failed: {failed}")

# # #
# Transform and load
# # #

def transform_and_load(**context: Any) -> dict[str, int]:
    conn = _get_connection()

    rows = conn.get_records("""
        SELECT DISTINCT ON (id) id, payload
        FROM raw.pokeapi
        ORDER BY id, ingested_at DESC
    """)

    loaded = 0
    failed = 0

    for pokemon_id, payload in rows:
        try:
            data = payload if isinstance(payload, dict) else json.loads(payload)
            stats = {s["stat"]["name"]: s["base_stat"] for s in data["stats"]}
            types = ",".join(t["type"]["name"] for t in data["types"])

            conn.run(
                """
                INSERT INTO curated.pokemon(
                    id, name, height_m, weight_kg, types,
                    hp, attack, defense, special_attack, special_defense, speed,
                    updated_at
                ) VALUES (
                    %(id)s, %(name)s, %(height)s, %(weight)s, %(types)s, 
                    %(hp)s, %(attack)s, %(defense)s, %(special_attack)s, 
                    %(special_defense)s, %(speed)s, NOW()
                )
                ON CONFLICT (id) DO UPDATE SET
                    name = EXCLUDED.name,
                    height_m = EXCLUDED.height_m,
                    weight_kg = EXCLUDED.weight_kg,
                    types = EXCLUDED.types,
                    hp = EXCLUDED.hp,
                    attack = EXCLUDED.attack,
                    defense = EXCLUDED.defense,
                    special_attack = EXCLUDED.special_attack,
                    special_defense = EXCLUDED.special_defense,
                    speed = EXCLUDED.speed,
                    updated_at = EXCLUDED.updated_at
                """,parameters={
                "id": pokemon_id, "name": data["name"],
                "height": data["height"]/10, "weight": data["weight"]/10,
                "types": types,
                "hp": stats.get("hp"), "attack": stats.get("attack"), 
                "defense": stats.get("defense"),
                "special_attack": stats.get("special-attack"), "special_defense": stats.get("special-defense"),
                "speed": stats.get("speed")
                }
            )
            loaded += 1
        except Exception as e:
            failed += 1
            logger.error(f"Failed to load pokemon {data["name"]}: {e}")


with DAG(
    dag_id=dag_name,
    schedule="@daily",
    start_date=datetime(2026, 6, 1),
    catchup=False,
    default_args={
        "owner":"involves"
    }
) as dag:
    setup = PythonOperator(
        task_id="create_tables",
        python_callable=create_tables
    )

    permissions = PythonOperator(
        task_id="set_permissions",
        python_callable=set_app_permissions
    )

    extraction = PythonOperator(
        task_id="extraction",
        python_callable=extract
    )

    transform = PythonOperator(
        task_id="transform",
        python_callable=transform_and_load
    )

    setup >> permissions >> extraction >> transform
