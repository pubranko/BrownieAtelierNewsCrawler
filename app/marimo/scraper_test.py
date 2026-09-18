import marimo
from bs4 import BeautifulSoup as bs4

__generated_with = "0.24.0"
app = marimo.App()


@app.cell
def _():
    import json
    import os
    import sys
    from pathlib import Path
    from pprint import pprint

    project_root = Path(__file__).resolve().parents[2]
    app_dir = project_root / "app"
    if str(app_dir) not in sys.path:
        sys.path.append(str(app_dir))
    launch = project_root / ".vscode" / "jupyter_env.json"
    print(f"=== 設定ファイル: {launch}")
    with open(launch) as f:
        file = f.read()
    launch_json: dict = json.loads(file)

    for key, value in launch_json.items():
        os.environ[key] = value
    return (pprint,)


@app.cell
def _():
    import requests
    from prefect_lib.scraper.publish_date_scraper import scraper as publish_date_scraper

    return bs4, publish_date_scraper, requests


@app.cell
def _(bs4, requests):
    """単体テスト用の設定"""
    # test_url = 'https://mainichi.jp/articles/20220605/k00/00m/030/136000c'
    test_url = "https://www.kyodo.co.jp/sponsored/2026-09-01_4032310/"
    # test_url = 'https://www.kyodo.co.jp/life/2024-10-30_3892147/'
    # 通常サイト用
    request = requests.get(test_url)
    # bs4で解析
    soup: bs4 = bs4(request.text, "lxml")
    return (soup,)


@app.cell
def _():
    scrape_parm = [
        {
            "pattern": 3,
            "css_selecter": "section[class=post_ttl] time[class=post_detail__date]",
            "priority": 3,
            "register_date": "2024-04-30T16:00:00+09:00",
        },
        {
            "pattern": 2,
            "css_selecter": 'head > meta[name="iso-8601-publish-date"]',
            "priority": 2,
            "register_date": "2022-04-16T14:00:00+09:00",
        },
        {
            "pattern": 1,
            "css_selecter": 'head > meta[name="iso-8601-modified-date"]',
            "priority": 1,
            "register_date": "2022-04-16T14:00:00+09:00",
        },
    ]
    return (scrape_parm,)


@app.cell
def _(pprint, publish_date_scraper, scrape_parm, soup: bs4):
    scrape_parm_1 = sorted(scrape_parm, key=lambda d: d["pattern"], reverse=True)
    # pprint(f'=== scrape_parm === \n{scrape_parm}')
    result = publish_date_scraper(soup=soup, scraper="publish_date_scraper", scrape_parm=scrape_parm_1)
    # result = airticle_scraper(
    #     soup=soup,
    #     scraper='article_scraper',
    #     scrape_parm=scrape_parm,
    # )
    pprint(f"=== result === \n{result}")
    return


if __name__ == "__main__":
    app.run()
