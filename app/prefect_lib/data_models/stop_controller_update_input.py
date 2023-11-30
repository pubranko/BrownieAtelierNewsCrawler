from typing import Any, Final
from pydantic import BaseModel, validator, Field


class StopControllerUpdateConst:
    '''Stop Controller Update Flow用コンスタントクラス'''

    DOMAIN:Final[str] = 'domain'
    '''パラメータ名:domain'''

    COMMAND:Final[str] = 'command'
    '''パラメータ名:command'''
    COMMAND__ADD:Final[str] = 'add'
    '''パラメータ定数：コマンド：追加'''
    COMMAND__DELETE:Final[str] = 'delete'
    '''パラメータ定数：コマンド：削除'''

    DESTINATION:Final[str] = 'destination'
    '''パラメータ名:destination'''
    DESTINATION__CRAWLING: Final[str] = 'crawling'
    '''パラメータ定数：指定場所：停止対象：クローリング'''
    DESTINATION__SCRAPYING: Final[str] = 'scrapying'
    '''パラメータ定数：指定場所：停止対象：スクレイピング'''


class StopControllerUpdateInput(BaseModel):
    domain:str = Field(..., title="ドメイン")
    command: str = Field(..., title="コマンド")
    destination: str = Field(..., title="指定先")


    def __init__(self, **data: Any):
        '''あとで'''
        super().__init__(**data)

    '''
    定義順にチェックされる。
    valuesにはチェック済みの値のみが入るため順序は重要。(単項目チェック、関連項目チェックの順で定義するのが良さそう。)
    '''
    ##################################
    # 単項目チェック、省略時の値設定
    ##################################
    @validator(StopControllerUpdateConst.COMMAND)
    def report_term_check(cls, value: str, values: dict) -> str:
        if value not in [StopControllerUpdateConst.COMMAND__ADD, StopControllerUpdateConst.COMMAND__DELETE]:
            raise ValueError(
                f'コマンド指定ミス。{StopControllerUpdateConst.COMMAND__ADD}, {StopControllerUpdateConst.COMMAND__DELETE} のいずれかで入力してください。')
        return value

    @validator(StopControllerUpdateConst.DESTINATION)
    def totalling_term_check(cls, value: str, values: dict) -> str:
        if value not in [StopControllerUpdateConst.DESTINATION__CRAWLING, StopControllerUpdateConst.DESTINATION__SCRAPYING]:
            raise ValueError(
                f'指定先の指定ミス。{StopControllerUpdateConst.DESTINATION__CRAWLING}, {StopControllerUpdateConst.DESTINATION__SCRAPYING} のいずれかで入力してください。')
        return value

    ###################################
    # 関連項目チェック
    ###################################
