import marimo

__generated_with = "0.24.0"
app = marimo.App()


@app.cell
def _():
    import json
    import os
    import sys
    from pathlib import Path

    project_root = Path(__file__).resolve().parents[2]
    app_dir = project_root / "app"
    if str(app_dir) not in sys.path:
        sys.path.append(str(app_dir))
    launch = project_root / ".vscode" / "jupyter_env.json"
    print(f"=== 設定ファイル: {launch}")
    with open(launch) as f:
        _file = f.read()
    launch_json: dict = json.loads(_file)
    for key, value in launch_json.items():
        os.environ[key] = value
    return (os,)


@app.cell
def _():
    import logging
    import pickle
    from logging import Logger
    from typing import Any

    from BrownieAtelierMongo.collection_models.crawler_response_model import CrawlerResponseModel
    from BrownieAtelierMongo.collection_models.mongo_model import MongoModel
    from bs4 import BeautifulSoup as bs4
    from prefect_lib.scraper.article_scraper import scraper as artcle_scraper
    from pymongo import ASCENDING
    from shared.settings import DATA__DEBUG_FILE_DIR

    return (
        ASCENDING,
        Any,
        CrawlerResponseModel,
        DATA__DEBUG_FILE_DIR,
        Logger,
        MongoModel,
        artcle_scraper,
        bs4,
        logging,
        pickle,
    )


@app.cell
def _(
    Any,
    CrawlerResponseModel,
    Logger,
    MongoModel,
    logging,
):
    logger: Logger = logging.getLogger("prefect.run.scrapying_deco")

    mongo: MongoModel = MongoModel()
    crawler_response: CrawlerResponseModel = CrawlerResponseModel(mongo)

    conditions: list = []
    urls: list[str] = [
        "https://mainichi.jp/articles/20220605/k00/00m/030/136000c",
    ]
    scrape_parm = [
        {
            "pattern": 1,
            "css_selecter": 'head > meta[name="pubdate"]',
        }
    ]

    conditions.append({"url": {"$in": urls}})
    if conditions:
        filter: Any = {"$and": conditions}
    else:
        filter = None
    logger.info(f"=== crawler_responseへのfilter: {str(filter)}")
    return crawler_response, filter, scrape_parm


@app.cell
def _(crawler_response, filter):
    # スクレイピング対象件数を確認
    record_count = crawler_response.count(filter=filter)
    print("=== 件数 ", record_count)
    return


@app.cell
def _(
    ASCENDING,
    DATA__DEBUG_FILE_DIR,
    artcle_scraper,
    bs4,
    crawler_response,
    filter,
    os,
    pickle,
    scrape_parm,
):
    records = crawler_response.find(
        projection=None,
        filter=filter,
        sort=[("domain", ASCENDING), ("response_time", ASCENDING)],
    )
    for record in records:
        print("record: ", record["url"])
        response_body = pickle.loads(record["response_body"])
        soup = bs4(response_body, "lxml")
        path = os.path.join(DATA__DEBUG_FILE_DIR, "response_data.html")
        with open(path, "w") as file:
            file.write(str(soup.select_one("html")))
        scrape_parm_1 = sorted(scrape_parm, key=lambda d: d["pattern"], reverse=True)
        print("\n\n=== scrape_parm ===", scrape_parm_1)
        result = artcle_scraper(soup=soup, scraper="artcle_scraper", scrape_parm=scrape_parm_1)
        print("\n\n=== result ===", result)
    return


if __name__ == "__main__":
    app.run()
