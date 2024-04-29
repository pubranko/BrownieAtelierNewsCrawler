#!/bin/bash

# python仮想環境を有効化
. .venv/bin/activate
which python

# テスト用のシェルを起動
cd app
# python prefect_lib/test/github_actions_unit_test.py
# python prefect_lib/github_actions_unit_tests/github_actions_unit_test_01.py
python $UNIT_TEST_FILE

