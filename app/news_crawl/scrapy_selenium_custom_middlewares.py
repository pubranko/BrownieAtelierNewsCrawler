"""This module contains the ``SeleniumMiddleware`` scrapy middleware"""
import os
# from logging import logger,INFO
import subprocess
from importlib import import_module
from typing import Optional

from scrapy import signals
from scrapy.exceptions import NotConfigured
from scrapy.http import HtmlResponse
from scrapy_selenium.http import SeleniumRequest
from selenium import webdriver
from selenium.webdriver import FirefoxOptions
from selenium.webdriver.firefox.firefox_profile import FirefoxProfile
from selenium.webdriver.firefox.service import Service
from selenium.webdriver.firefox.webdriver import WebDriver
from selenium.webdriver.support.wait import WebDriverWait


class SeleniumMiddleware:
    """Scrapy middleware handling the requests using selenium"""

    driver: WebDriver

    def __init__(
        self,
        driver_name: str,
        driver_executable_path: str,
        driver_arguments: list,
        browser_executable_path: str,
        profile: Optional[FirefoxProfile],
    ):  # パラメータにプロファイルを追加してみた。
        """Initialize the selenium webdriver

        Parameters
        ----------
        driver_name: str
            The selenium ``WebDriver`` to use
        driver_executable_path: str
            The path of the executable binary of the driver
        driver_arguments: list
            A list of arguments to initialize the driver
        browser_executable_path: str
            The path of the executable binary of the browser
        """

        webdriver_base_path = f"selenium.webdriver.{driver_name}"

        # カスタム 型ヒント
        driver_options: FirefoxOptions = FirefoxOptions()

        for argument in driver_arguments:
            driver_options.add_argument(argument)

        if profile:
            driver_options.profile = profile  # 追加されたパラメータのプロファイルを設定
        # driver_options.log.level = "INFO"

        service = Service(
            executable_path=driver_executable_path,
            port=0,
            service_args=["--log", "info"],
            log_output=subprocess.DEVNULL,
            env=None,
        )

        self.driver = webdriver.Firefox(
            options=driver_options,
            service=service,
        )
        # ヘッドレスモード時のウィンドウサイズを強制する
        self.driver.set_window_size(1920, 1080)  # コンテナ環境用のサイズ指定

    @classmethod
    def from_crawler(cls, crawler):
        """Initialize the middleware with the crawler settings"""

        driver_name = crawler.settings.get("SELENIUM_DRIVER_NAME")
        driver_executable_path = crawler.settings.get("SELENIUM_DRIVER_EXECUTABLE_PATH")
        browser_executable_path = crawler.settings.get(
            "SELENIUM_BROWSER_EXECUTABLE_PATH"
        )
        driver_arguments = crawler.settings.get("SELENIUM_DRIVER_ARGUMENTS")

        if not driver_name or not driver_executable_path:
            raise NotConfigured(
                "SELENIUM_DRIVER_NAME and SELENIUM_DRIVER_EXECUTABLE_PATH must be set"
            )

        # firefox用のプロファイルを作成してミドルウェアのインスタンス作成時に
        # それを使用するようカスタマイズ
        set_preferences: dict[str, int] = crawler.settings.get(
            "SELENIUM_DRIVER_SET_PREFERENCE"
        )
        new_profile = FirefoxProfile(
            profile_directory=crawler.settings.get("SELENIUM_FIREFOX_PROFILE_DIRECTORY")
        )
        for key, value in set_preferences.items():
            new_profile.set_preference(key, value)
        # ここで当ミドルウェアのインスタンス化を行っている。
        middleware = cls(
            driver_name=driver_name,
            driver_executable_path=driver_executable_path,
            driver_arguments=driver_arguments,
            browser_executable_path=browser_executable_path,
            profile=new_profile,  # パラメータにプロファイルを追加してみた。
        )

        crawler.signals.connect(middleware.spider_closed, signals.spider_closed)

        return middleware

    def process_request(self, request, spider):
        """Process a request using the selenium driver if applicable"""

        if not isinstance(request, SeleniumRequest):
            return None

        # ここで実際にブラウザーでリクエストを実行しているっぽい。
        self.driver.get(request.url)

        # 型ヒントでエラーとなるためカスタマイズ
        cookies: dict = {}
        if type(request.cookies) is list:
            cookies: dict = request.cookies[0]
        elif type(request.cookies) is dict:
            cookies: dict = request.cookies

        for cookie_name, cookie_value in cookies.items():
            self.driver.add_cookie({"name": cookie_name, "value": cookie_value})

        if request.wait_until:
            WebDriverWait(self.driver, float(request.wait_time or 0)).until(
                request.wait_until
            )

        if request.screenshot:
            request.meta["screenshot"] = self.driver.get_screenshot_as_png()

        if request.script:
            self.driver.execute_script(request.script)

        body = str.encode(self.driver.page_source)

        # Expose the driver via the "meta" attribute
        request.meta.update({"driver": self.driver})

        return HtmlResponse(
            self.driver.current_url, body=body, encoding="utf-8", request=request
        )

    def spider_closed(self):
        """Shutdown the driver when spider is closed"""

        self.driver.quit()
