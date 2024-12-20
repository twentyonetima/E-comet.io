from app.db.connection import get_db_connection

async def fetch_top_repos(sort_by="stars"):
    """Получение top-100 repos из бд."""
    query = f"""
    SELECT repo, owner, position_cur, position_prev, stars, watchers, forks, open_issues, language
    FROM top100
    ORDER BY {sort_by} DESC
    LIMIT 100;
    """
    conn = await get_db_connection()
    try:
        return await conn.fetch(query)
    finally:
        await conn.close()


async def fetch_repo_activity(owner, repo, since, until):
    """Получение активности repo из бд."""
    query = """
    SELECT date, commits, authors
    FROM activity
    WHERE owner = $1 AND repo = $2 AND date BETWEEN $3 AND $4;
    """
    conn = await get_db_connection()
    try:
        rows = await conn.fetch(query, owner, repo, since, until)
        return [
            {"date": row["date"], "commits": row["commits"], "authors": row["authors"]}
            for row in rows
        ]
    finally:
        await conn.close()