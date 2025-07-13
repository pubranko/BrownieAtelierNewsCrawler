import glob
import json
import os

from BrownieAtelierMongo.collection_models.mongo_model import MongoModel
from BrownieAtelierMongo.collection_models.scraper_info_by_domain_model import \
    ScraperInfoByDomainModel
from BrownieAtelierMongo.data_models.scraper_info_by_domain_data import \
    ScraperInfoByDomainConst
from prefect import get_run_logger, task
from prefect.cache_policies import NO_CACHE
from pydantic import ValidationError
from shared.settings import DATA__SCRAPER_INFO_BY_DOMAIN_DIR


@task(cache_policy=NO_CACHE)
def scraper_info_by_domain_task(scraper_info_by_domain_files: list, mongo: MongoModel):
    logger = get_run_logger()  # PrefectLogAdapter
    logger.info(f"=== 引数: {scraper_info_by_domain_files}")

    scraper_info_by_domain_model = ScraperInfoByDomainModel(mongo)

    get_files: list = []
    if len(scraper_info_by_domain_files) == 0:
        path = os.path.join(DATA__SCRAPER_INFO_BY_DOMAIN_DIR, "*.json")
        get_files = glob.glob(path)
        if len(get_files) == 0:
            # raise ENDRUN(state=state.Failed())
            raise IOError(
                f"対象ディレクトリにファイルが見つかりませんでした。ディレクトリにファイルを格納してください。 (ディレクトリ= {DATA__SCRAPER_INFO_BY_DOMAIN_DIR})"
            )
        else:
            logger.info(f"=== ファイル指定なし → 全ファイル対象 : {get_files}")
    else:
        for file in scraper_info_by_domain_files:
            file_path = os.path.join(DATA__SCRAPER_INFO_BY_DOMAIN_DIR, file)
            if os.path.exists(file_path):
                raise IOError(
                    f"対象ディレクトリにファイルが見つかりませんでした。ファイル名に誤りがある可能性があります。 (ディレクトリ= {DATA__SCRAPER_INFO_BY_DOMAIN_DIR}, ファイル名= {file})"
                )
            get_files.append(file_path)

    for file_path in get_files:
        logger.info(f"=== ファイルチェック : {file_path}")
        with open(file_path, "r") as f:
            file = f.read()

        scraper_info: dict = json.loads(file)

        try:
            scraper_info_by_domain_model.data_check(scraper=scraper_info)
        except ValidationError as e:
            error_info: list = e.errors()
            logger.error(f'=== エラー({file_path}) : {error_info[0]["msg"]}')
        else:
            scraper_info_by_domain_model.update_one(
                filter={
                    ScraperInfoByDomainConst.DOMAIN: scraper_info[
                        ScraperInfoByDomainConst.DOMAIN
                    ]
                },
                record={"$set": scraper_info},
            )
            logger.info(f"=== 登録完了 : {file_path}")

        # 処理の終わったファイルオブジェクトを削除
        del file, scraper_info

