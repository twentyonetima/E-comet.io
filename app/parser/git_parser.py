import aiohttp

from app.db.save import save_activity_to_db
from app.db.update import update_positions_in_top_100

ACTIVITIES_API_URL = "https://functions.yandexcloud.net/d4ei370aq9gh79u190b7"
TOP_100_API_URL = "https://functions.yandexcloud.net/d4ed8hfm7qsjr6eo7gvj"


async def get_top_100_repos():
    async with aiohttp.ClientSession() as session:
        async with session.get(TOP_100_API_URL) as response:
            if response.status == 200:
                try:
                    response_data = await response.json()
                    return response_data.get("repositories", {}).get("items", [])
                except Exception as e:
                    raise e
            else:
                raise Exception(f"Yandex Cloud error: {await response.text()}")


async def get_and_update_top_100_repos():
    """Получение top-100 repos и обновление позиций в db"""
    repositories = await get_top_100_repos()
    await update_positions_in_top_100(repositories)


async def get_repo_activity(owner, repo, since, until):
    """Получение активности repo за выбранный промежуток времени"""
    since_str = since.strftime("%Y-%m-%d")
    until_str = until.strftime("%Y-%m-%d")
    print(since_str, type(since_str))
    params = {
        "owner": owner,
        "repo": repo,
        "since": since_str,
        "until": until_str
    }

    async with aiohttp.ClientSession() as session:
        async with session.get(ACTIVITIES_API_URL, params=params) as response:
            if response.status == 200:
                data = await response.json()
                return data.get("activity")
            else:
                raise Exception(f"Yandex Cloud Function error: {await response.text()}")


async def update_activity_in_db(owner, repo, since, until):
    """Сохрание активности репозетория за выбранный диапазон в бд"""
    activity = await get_repo_activity(owner, repo, since, until)
    await save_activity_to_db(activity, owner, repo)
