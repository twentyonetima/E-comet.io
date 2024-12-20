import asyncpg
import os
from dotenv import load_dotenv

load_dotenv()


async def get_db_connection():
    return await asyncpg.connect(
        user=os.getenv("DB_USER"),
        password=os.getenv("DB_PASSWORD"),
        database=os.getenv("DB_NAME"),
        host=os.getenv("DB_HOST"),
        port=os.getenv("DB_PORT"),
    )


async def close_db(connection):
    if connection:
        await connection.close()