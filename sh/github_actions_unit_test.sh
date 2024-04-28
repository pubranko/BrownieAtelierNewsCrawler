#!/bin/bash

# python仮想環境を有効化
cd $HOME/BrownieAtelier/
. .venv/bin/activate

# テスト用のシェルを起動
cd $HOME/BrownieAtelier/app
python prefect_lib/test/github_actions_unit_test.py
