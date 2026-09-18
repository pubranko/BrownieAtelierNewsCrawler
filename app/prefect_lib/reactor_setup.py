"""旧来の同期クロールタスク向けに、Scrapy import 前にリアクターを設定する。"""

import importlib
import sys

if "twisted.internet.reactor" in sys.modules:
    del sys.modules["twisted.internet.reactor"]
asyncioreactor = importlib.import_module("twisted.internet.asyncioreactor")
asyncioreactor.install()
reactor = importlib.import_module("twisted.internet.reactor")
