from app.db.connection import get_db_connection


async def update_positions_in_top_100(repositories):
    """Обновление текущей и предыдущей позиции"""
    connection = await get_db_connection()
    try:
        async with connection.transaction():
            await connection.execute(""" 
                CREATE TEMP TABLE temp_top100 (
                    repo TEXT PRIMARY KEY,
                    position_cur INTEGER,
                    stars INTEGER,
                    watchers INTEGER,
                    forks INTEGER,
                    open_issues INTEGER,
                    language TEXT
                ) ON COMMIT DROP;
            """)

            temp_data = [
                (
                    repo["full_name"],
                    index + 1,
                    repo["stargazers_count"],
                    repo["watchers_count"],
                    repo["forks_count"],
                    repo["open_issues_count"],
                    repo.get("language"),
                )
                for index, repo in enumerate(repositories)
            ]

            await connection.copy_records_to_table('temp_top100', records=temp_data)

            await connection.execute(""" 
                INSERT INTO top100 (repo, owner, position_cur, position_prev, stars, watchers, forks, open_issues, language)
                SELECT
                    temp.repo,
                    COALESCE(t.owner, '') AS owner,
                    temp.position_cur,
                    t.position_cur AS position_prev,  -- Use the previous position
                    temp.stars,
                    temp.watchers,
                    temp.forks,
                    temp.open_issues,
                    temp.language
                FROM temp_top100 temp
                LEFT JOIN top100 t ON temp.repo = t.repo
                ON CONFLICT (repo) DO UPDATE
                SET
                    position_prev = EXCLUDED.position_prev,
                    position_cur = EXCLUDED.position_cur,
                    stars = EXCLUDED.stars,
                    watchers = EXCLUDED.watchers,
                    forks = EXCLUDED.forks,
                    open_issues = EXCLUDED.open_issues,
                    language = EXCLUDED.language;
            """)
    except Exception as e:
        raise e
    finally:
        await connection.close()
