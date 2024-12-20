from fastapi import APIRouter, Query, HTTPException
from app.db.db_queries import fetch_top_repos, fetch_repo_activity
from app.parser.git_parser import get_top_100_repos, update_activity_in_db
from app.db.save import save_top_100_to_db
from typing import Literal
from datetime import datetime

router = APIRouter()


@router.get("/api/repos/top100")
async def get_top_repositories(sort_by: Literal["stars", "forks", "watchers"] = "stars"):
    try:
        records = await fetch_top_repos(sort_by)
        if not records:
            repos = await get_top_100_repos()
            if not repos:
                raise HTTPException(status_code=404, detail="Repos not found")

            await save_top_100_to_db(repos)
            records = await fetch_top_repos(sort_by)
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Ошибка получения или сохранения данных: {str(e)}")

    return {"data": [dict(record) for record in records]}


@router.get("/api/repos/{owner}/{repo}/activity")
async def get_repo_activity(
    owner: str,
    repo: str,
    since: str = Query(..., description="Дата начала в формате YYYY-MM-DD"),
    until: str = Query(..., description="Дата окончания в формате YYYY-MM-DD")
):
    try:
        since_date = datetime.strptime(since, "%Y-%m-%d").date()
        until_date = datetime.strptime(until, "%Y-%m-%d").date()
    except ValueError:
        raise HTTPException(status_code=400, detail="Неверный формат дат")

    if since_date > until_date:
        raise HTTPException(status_code=400, detail="Дата 'since' должна быть раньше 'until'")

    try:
        activity_data = await fetch_repo_activity(owner, repo, since_date, until_date)

        if not activity_data:
            await update_activity_in_db(owner, repo, since_date, until_date)
            activity_data = await fetch_repo_activity(owner, repo, since_date, until_date)

        if not activity_data:
            raise HTTPException(status_code=404, detail="Нет данных об активности за указанный период")

        return {"activity": activity_data}
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Ошибка при получении данных: {str(e)}")
