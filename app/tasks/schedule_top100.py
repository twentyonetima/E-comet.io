import schedule
import asyncio
from app.parser.git_parser import get_and_update_top_100_repos
from app.db.connection import get_db_connection


async def update_positions_task():
    """Task для получения и сохранения новых позиций в бд."""
    try:
        connection = await get_db_connection()
        try:
            await get_and_update_top_100_repos()
        finally:
            await connection.close()
    except Exception as e:
        if str(e) == 'Yandex Cloud error: {"error":"Cannot connect to host api.github.com:443 ssl:default [Temporary failure in name resolution]':
            await update_positions_task()
        else:
            raise e


async def scheduler_loop():
    """Запускает задачи по расписанию."""
    while True:
        schedule.run_pending()
        await asyncio.sleep(60)


async def start_scheduler():
    """Инициализирует планировщик и выполняет задачи по расписанию."""
    schedule.every().day.at("02:00").do(lambda: asyncio.create_task(update_positions_task()))
    try:
        await scheduler_loop()
    except Exception as e:
        raise e

