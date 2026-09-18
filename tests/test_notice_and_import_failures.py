"""外部サービスに接続せず、認証失敗と未対応コレクションの扱いを検証する。"""

import gzip
import importlib
import os
import tempfile
import unittest
from pathlib import Path
from unittest.mock import Mock, patch

import bson
from BrownieAtelierNotice.slack.slack_notice import slack_notice


class SlackNoticeTests(unittest.TestCase):
    def test_auth_failure_stops_message_and_file_delivery(self):
        for attachment in ("", b"attachment"):
            with (
                self.subTest(attachment=attachment),
                patch("BrownieAtelierNotice.slack.slack_notice.WebClient") as client_class,
            ):
                client = client_class.return_value
                client.auth_test.side_effect = RuntimeError("authentication unavailable")
                logger = Mock()
                slack_notice(logger, "test-channel", "test-message", file=attachment)
                logger.critical.assert_called_once()
                self.assertTrue(logger.critical.call_args.kwargs["exc_info"])
                client.chat_postMessage.assert_not_called()
                client.files_upload_v2.assert_not_called()

    def test_unsuccessful_auth_response_stops_delivery(self):
        with patch("BrownieAtelierNotice.slack.slack_notice.WebClient") as client_class:
            client = client_class.return_value
            client.auth_test.return_value = {"ok": False}
            logger = Mock()
            slack_notice(logger, "test-channel", "test-message")
            logger.critical.assert_called_once()
            client.chat_postMessage.assert_not_called()
            client.files_upload_v2.assert_not_called()

    def test_successful_auth_sends_message(self):
        with patch("BrownieAtelierNotice.slack.slack_notice.WebClient") as client_class:
            client = client_class.return_value
            client.auth_test.return_value = {"ok": True}
            client.chat_postMessage.return_value = {"ok": True}
            slack_notice(Mock(), "test-channel", "test-message")
            client.chat_postMessage.assert_called_once_with(channel="test-channel", text="test-message")


class MongoImportTests(unittest.TestCase):
    def setUp(self):
        test_env = {
            f"BROWNIE_ATELIER_MONGO__MONGO_{key}": "test"
            for key in ("SERVER", "PORT", "USE_DB", "USER", "PASS", "TLS_CA_FILE", "TLS_CERTTIFICATE_KEY_FILE")
        }
        with patch.dict(os.environ, test_env):
            self.module = importlib.import_module("prefect_lib.tasks.mongo_import_task")
        self.directory = tempfile.TemporaryDirectory()
        self.addCleanup(self.directory.cleanup)
        self.root = Path(self.directory.name)
        (self.root / "backup").mkdir()
        self.enterContext(patch.object(self.module, "DATA__BACKUP_BASE_DIR", str(self.root)))
        self.enterContext(patch.object(self.module, "get_run_logger", return_value=Mock()))
        self.collection_class = self.enterContext(patch.object(self.module, "CrawlerResponseModel"))
        self.collection_class.COLLECTION_NAME = "crawler_response"

    def write_backup(self, name):
        with gzip.open(self.root / "backup" / f"{name}.gz", "wb") as output:
            output.write(bson.BSON.encode({"_id": "old-id", "value": name}))

    def test_unknown_first_collection_raises_before_reading_backup(self):
        self.write_backup("unsupported")
        with patch.object(self.module.gzip, "open") as open_backup:
            with self.assertRaisesRegex(ValueError, "unsupported"):
                self.module.mongo_import_task.fn(Mock(), "backup", ["unsupported"])
            open_backup.assert_not_called()
        self.collection_class.assert_not_called()

    def test_unknown_collection_does_not_reuse_previous_destination(self):
        self.write_backup("crawler_response")
        self.write_backup("unsupported")
        with self.assertRaisesRegex(ValueError, "unsupported"):
            self.module.mongo_import_task.fn(Mock(), "backup", ["crawler_response", "unsupported"])
        self.collection_class.return_value.insert.assert_called_once_with([{"value": "crawler_response"}])

    def test_supported_collection_imports_without_preserving_id(self):
        self.write_backup("crawler_response")
        self.module.mongo_import_task.fn(Mock(), "backup", ["crawler_response"])
        self.collection_class.return_value.insert.assert_called_once_with([{"value": "crawler_response"}])


if __name__ == "__main__":
    unittest.main()
