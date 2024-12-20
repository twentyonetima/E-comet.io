from datetime import datetime
from app.db.connection import get_db_connection


async def save_top_100_to_db(repos):
    connection = await get_db_connection()
    insert_query = """
        INSERT INTO top100 (repo, owner, position_cur, position_prev, stars, watchers, forks, open_issues, language)
        VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9)
        ON CONFLICT (repo) DO UPDATE SET
            position_prev = EXCLUDED.position_cur,
            position_cur = EXCLUDED.position_cur,
            stars = EXCLUDED.stars,
            watchers = EXCLUDED.watchers,
            forks = EXCLUDED.forks,
            open_issues = EXCLUDED.open_issues,
            language = EXCLUDED.language;
    """

    try:
        async with connection.transaction():
            for index, repo in enumerate(repos):
                data = (
                    repo["full_name"],
                    repo["owner"]["login"],
                    index + 1,
                    None,
                    repo["stargazers_count"],
                    repo["watchers_count"],
                    repo["forks_count"],
                    repo["open_issues_count"],
                    repo.get("language")
                )
                await connection.execute(insert_query, *data)
    except Exception as e:
        raise e
    finally:
        await connection.close()


async def save_activity_to_db(activity, owner, repo):
    connection = await get_db_connection()
    insert_query = """
        INSERT INTO activity (date, commits, authors, repo, owner)
        VALUES ($1, $2, $3, $4, $5)
        ON CONFLICT (date, repo, owner) DO UPDATE SET
            commits = EXCLUDED.commits,
            authors = EXCLUDED.authors;
    """

    try:
        async with connection.transaction():
            for activity_item in activity:
                if isinstance(activity_item["date"], str):
                    activity_item["date"] = datetime.strptime(activity_item["date"], "%Y-%m-%d").date()

                data = (
                    activity_item["date"],
                    activity_item["commits"],
                    activity_item["authors"],
                    repo,
                    owner
                )
                await connection.execute(insert_query, *data)
    except Exception as e:
        raise e
    finally:
        await connection.close()
