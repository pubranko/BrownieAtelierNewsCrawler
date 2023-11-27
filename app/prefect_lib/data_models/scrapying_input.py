from datetime import datetime
from typing import Any, Optional, Final, Literal
from pydantic import BaseModel, Field

CONST__DOMAIN: Final[str] = 'domain'
CONST__TARGET_START_TIME_FROM: Final[str] = 'target_start_time_from'
CONST__TARGET_START_TIME_TO: Final[str] = 'target_start_time_to'
CONST__URLS: Final[str] = 'urls'
CONST__FOLLOWING_PROCESSING_EXECUTION: Final[str] = 'following_processing_execution'

class ScrapyingInput(BaseModel):
    domain: Optional[str] = Field(None, title='')
    target_start_time_from: Optional[datetime] = Field(None, title='')
    target_start_time_to: Optional[datetime] = Field(None, title='')
    urls: Optional[list[str]] = Field(None, title='')
    following_processing_execution: Optional[bool] = Field(None, title='')

    #####################
    # 定数
    #####################
    DOMAIN: str = Literal[f'{CONST__DOMAIN}']
    '''定数: domain'''
    TARGET_START_TIME_FROM: str = Literal[f'{CONST__TARGET_START_TIME_FROM}']
    '''定数: target_start_time_from'''
    TARGET_START_TIME_TO: str = Literal[f'{CONST__TARGET_START_TIME_TO}']
    '''定数: target_start_time_to'''
    URLS: str = Literal[f'{CONST__URLS}']
    '''定数: urls'''
    FOLLOWING_PROCESSING_EXECUTION: bool = Literal[f'{CONST__FOLLOWING_PROCESSING_EXECUTION}']
    '''定数: following_processing_execution'''


    def __init__(self, **data: Any):
        '''引数チェッククラス。以下のFlowで使用。
        ①Scrapying Flow
        ②Scraped news clip master save flow
        '''
        super().__init__(**data)

    '''
    クラス変数側の定義順にチェックされる。
    valuesにはチェック済みの値のみが入るため順序は重要。(単項目チェック、関連項目チェックの順で定義するのが良さそう。)
    値がNoneの場合、以下のチェックは動かない。Noneでも動かす場合、「always=True」指定で動かすことができる。例）@validator('aaa', always=True)
    通常上記の型チェックが先に動く。型チェックの前に動かすには「pre=True」指定で動かすことができる。例）@validator('aaa', pre=True, always=True)
    '''
    ##################################
    # 単項目チェック、省略時の値設定
    ##################################

    ###################################
    # 関連項目チェック
    ###################################

    #####################################
    # カスタマイズデータ
    #####################################
