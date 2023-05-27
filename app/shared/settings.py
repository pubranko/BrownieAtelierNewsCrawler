import os
from typing import Final
from datetime import timedelta, timezone
from decouple import config, AutoConfig
# .envファイルが存在するパスを指定。実行時のカレントディレクトリに.envを配置している場合、以下の設定不要。
# config = AutoConfig(search_path="./shared")


TIMEZONE = timezone(timedelta(hours=9), 'JST')
'''タイムゾーン'''

DATA_DIR = os.path.abspath(str(config('PREFECT__DATA_DIR_PATH', default='data_dir')))
'''データ類の保存ベースディレクトリ'''

DATA_DIR__LOGS = os.path.join(DATA_DIR, 'logs')
'''ログの一時保存先'''
DATA_DIR__BACKUP_BASE_DIR:str = os.path.join(DATA_DIR, 'backup_files')
'''バックアップファイルを保存するベースディレクトリパス'''
DATA_DIR__DEBUG_FILE_DIR:str = os.path.join(DATA_DIR, 'debug')
'''デバック用ファイルの保存先'''
DATA_DIR__DIRECT_CRAWL_FILES_DIR:str = os.path.join(DATA_DIR, 'direct_crawl_files')
'''ダイレクトクロール用のファイルの格納先'''
DATA_DIR__SCRAPER_INFO_BY_DOMAIN_DIR:str = os.path.join(DATA_DIR, 'scraper_info_by_domain')
'''ドメイン別スクレイパーファイルの格納先'''

DATA_DIR__LOGIN_INFO:str = os.path.join(DATA_DIR, 'login_info')
'''クロール時にログインが必要なサイトのログイン情報の格納先ディレクトリ'''
DATA_DIR__LOGIN_INFO_YML:str = 'login_info.yml'
'''クロール時にログインが必要なサイトのログイン情報の格納先ファイル名(yml)'''

PREFECT_LIB__TASK_DIR: Final[str] = 'prefect_lib/task'
'''prefect_libのタスクを格納するディレクトリ'''



LOG_FORMAT = '%(asctime)s %(levelname)-7s [%(name)s] : %(message)s'
'''ログフォーマット'''
#LOG_FORMAT = '[%(asctime)s] %(levelname)s - %(name)s | %(message)s'
# 日付/時刻をフォーマットするための文字列、 LOG_FORMAT の %(asctime)s プレース・ホルダーの展開。
# 利用可能なディレクティブのリストについては、 Python datetime documentation を参照してください。
LOG_DATEFORMAT = '%Y-%m-%d %H:%M:%S'
'''ログ日時フォーマット'''
