import os
import requests
import json
import re
import sys
from typing import Union

"""
当スクリプトの呼び出し元へタグを文字列として返す。
呼び出し元では次のように利用することを想定している。
  export NEXT_TAG=$(python app/scripts/tag_create.py --mode TEST)
このNEXT_TAGを使用し、docker imageをビルドさせる。


new_crawlerイメージのtagのナンバリングについて
例）1.2.3
1.メジャーバージョン (Major Version):
    メジャーバージョンは、大規模な変更や互換性のない変更があった場合に上げられます。
    例えば、新しい機能の追加、既存機能の削除、APIの変更などが該当します。
    メジャーバージョンは通常、最初の番号として表されます（例: 1.0.0 の 1）。

2.マイナーバージョン (Minor Version):
    マイナーバージョンは、中程度の変更や新機能の追加があった場合に上げられます。
    例えば、既存機能の改善、新しいAPIエンドポイントの追加、パフォーマンスの向上などが該当します。
    マイナーバージョンは通常、2番目の番号として表されます（例: 1.2.0 の 2）。

3.パッチバージョン (Patch Version):
    パッチバージョンは、細かな修正やバグ修正があった場合に上げられます。
    例えば、セキュリティの修正、バグの修正、パフォーマンスの最適化などが該当します。
    パッチバージョンは通常、3番目の番号として表されます（例: 1.2.3 の 3）。

baseイメージのtagのナンバリングについて
例）16.1
1.メジャーバージョン (Major Version)、2.マイナーバージョン (Minor Version)のみとする。
内容は上記new_crawlerイメージと同様
"""

def init_check(
    args: list[str], docker_hub_username: Union[str, None], base_tag: Union[str, None]
) -> str:
    """引数チェック"""
    if len(args) != 3:
        raise ValueError(
            f"引数が指定されていません。「--mode TEST」等のようにモードを指定してください。"
        )
    if not args[1]:
        raise ValueError(f"第一引数に--modeが指定されていません。")
    if args[1] not in ["--mode", "-m"]:
        raise ValueError(
            f"第一引数のモード引数に誤りがあります。「--mode or -m」を指定してください。"
        )
    if args[2] not in ["PRODUCT", "TEST"]:
        raise ValueError(
            f"第二引数の環境指定に誤りがあります。「PRODUCT or TEST」を指定してください。"
        )

    if not docker_hub_username:
        raise ValueError(
            f"必要な環境変数(DOCKER_HUB_USERNAME)がありません。「DOCKER_HUB_USERNAME」にdockerhubのリポジトリのユーザー名を指定してください。"
        )

    if not base_tag:
        raise ValueError(
            f"必要な環境変数(BASE_TAG)がありません。「BASE_TAG」に現在のメジャーバージョン、マイナーバージョンを指定してください。(例: 1.15)"
        )

    pattern = r"^[test-]*\d*\.\d*$"  # nn.nn の形式
    if not re.match(pattern, str(base_tag)):
        raise ValueError(
            f"環境変数(BASE_TAG:{base_tag})の形式が不正です。「BASE_TAG」に現在のメジャーバージョン、マイナーバージョンを指定してください。(例: test-1.15、1.15)"
        )

    return args[2]


def dockerhub_tag_info_get(docker_hub_username: str) -> list:
    """Docker Hubよりタグ情報を取得して返す。"""
    response = requests.get(
        f"https://registry.hub.docker.com/v2/repositories/{docker_hub_username}/brownie-atelier-news-crawler/tags/"
    )

    docker_repository_info_json: str = response.text
    docker_repository_info: dict = json.loads(docker_repository_info_json)

    # 'results'キーがない場合（新規リポジトリ）は空リストとする
    images_info: list[dict] = docker_repository_info.get("results", [])
    tags_product: list = []
    pattern = r"^\d*\.\d*\.\d*$"  # nn.nn.nn の形式

    # base_tagはグローバル変数ではないため、引数で渡す必要がある
    # 既存の呼び出し側でbase_tagを渡すように修正する必要があるが、
    # ここではbase_tagが未定義の場合は全てのタグを返す
    base_tag = os.environ.get("BASE_TAG", "")

    for image_info in images_info:
        if str(image_info["name"]).startswith("test-"):
            # TEST環境用のイメージの場合
            pass

        elif re.match(pattern, str(image_info["name"])):
            # 本番環境用のイメージの場合
            if base_tag and str(image_info["name"]).startswith(f"{base_tag}"):
                tags_product.append(image_info["name"])

    return tags_product


def tag_create(mode: str, tags_product: list[str], base_tag: str) -> str:

    #################################################
    # docker hub上での現在の最大バージョンを求める。
    #################################################
    max_tag: dict[str, int] = {"major": 0, "minor": 0, "patch": 0}
    first_base_tag: bool = False if tags_product else True
    for tag in tags_product:
        product_major, product_minor, product_patch = tag.split(".")

        if int(product_major) > max_tag["major"]:
            # メジャーアップデートの場合、マイナー、パッチも併せてバージョンを更新
            max_tag["major"] = int(product_major)
            max_tag["minor"] = int(product_minor)
            max_tag["patch"] = int(product_patch)

        elif int(product_minor) > max_tag["minor"]:
            # マイナーアップデートの場合、パッチも併せてバージョンを更新
            max_tag["minor"] = int(product_minor)
            max_tag["patch"] = int(product_patch)

        elif int(product_patch) > max_tag["patch"]:
            # パッチアップデート
            max_tag["patch"] = int(product_patch)

    ########################################
    # baseタグと比較し今回のタグを決定する。
    ########################################
    base_major, base_minor = base_tag.replace("test-","").split(".")
    if int(base_major) > max_tag["major"]:
        # メジャーアップデートの場合、マイナー、パッチも併せてバージョンを更新
        max_tag["major"] = int(base_major)
        max_tag["minor"] = int(base_minor)
        max_tag["patch"] = 0

    elif int(base_minor) > max_tag["minor"]:
        # マイナーアップデートの場合、パッチも併せてバージョンを更新
        max_tag["minor"] = int(base_minor)
        max_tag["patch"] = 0

    else:
        # baseタグに変更が無ければ、パッチをカウントアップ
        max_tag["patch"] = max_tag["patch"] + 1

    # modeがTESTならばタグの頭に「test-」を付与する。
    prefix =  "test-" if mode == 'TEST' else ""

    return f'{prefix}{max_tag["major"]}.{max_tag["minor"]}.{max_tag["patch"]}'


if __name__ == "__main__":
    # 引数、環境変数を確認
    args = sys.argv
    docker_hub_username = os.environ.get("DOCKER_HUB_USERNAME")
    base_tag = os.environ.get("BASE_TAG")
    mode = init_check(args, docker_hub_username, base_tag)

    # docker hubよりタグの一覧情報を取得。
    tags_product = dockerhub_tag_info_get(str(docker_hub_username))

    #
    new_tag = tag_create(mode, tags_product, str(base_tag))

    # 当スクリプトの呼び出し元へタグを文字列として返す。
    # 呼び出し元では次のように利用することを想定している。
    #   export NEXT_TAG=$(python scripts/tag_create.py --mode TEST)
    # このNEXT_TAGを使用し、docker imageをビルドさせる。
    print(new_tag)




"""
response.textの中のjsonデータイメージ。
resultsのリスト内にタグごとの情報が格納されている。

{
    "count": 3,
    "next": null,
    "previous": null,
    "results": [
        {
            "creator": 6553388,
            "id": 657411116,
            "images": [
                {
                    "architecture": "amd64",
                    "features": "",
                    "variant": null,
                    "digest": "sha256:d45c90ec473226597a2a4dae636b63f0ec06b713cd3233b865d77937fdaa03e0",
                    "os": "linux",
                    "os_features": "",
                    "os_version": null,
                    "size": 932075519,
                    "status": "active",
                    "last_pulled": null,
                    "last_pushed": "2024-05-14T11:00:47.871694Z"
                }
            ],
            "last_updated": "2024-05-14T11:00:47.871694Z",
            "last_updater": 6553388,
            "last_updater_username": "mikuras",
            "name": "test-0.15",
            "repository": 21775544,
            "full_size": 932075519,
            "v2": true,
            "tag_status": "active",
            "tag_last_pulled": null,
            "tag_last_pushed": "2024-05-14T11:00:47.871694Z",
            "media_type": "application/vnd.docker.container.image.v1+json",
            "content_type": "image",
            "digest": "sha256:d45c90ec473226597a2a4dae636b63f0ec06b713cd3233b865d77937fdaa03e0"
        },
        {
            "creator": 6553388,
            "id": 577315288,
            "images": [
                {
                    "architecture": "amd64",
                    "features": "",
                    "variant": null,
                    "digest": "sha256:c6a2222596d37028fcb292cc2f410cc47cefd255d88b1c40ee005a95e68dd53f",
                    "os": "linux",
                    "os_features": "",
                    "os_version": null,
                    "size": 769341656,
                    "status": "active",
                    "last_pulled": "2024-05-05T14:20:26.439707Z",
                    "last_pushed": "2024-01-02T15:37:43.223218Z"
                }
            ],
            "last_updated": "2024-01-02T15:37:43.329444Z",
            "last_updater": 6553388,
            "last_updater_username": "mikuras",
            "name": "test-0.14",
            "repository": 21775544,
            "full_size": 769341656,
            "v2": true,
            "tag_status": "active",
            "tag_last_pulled": "2024-05-05T14:20:26.439707Z",
            "tag_last_pushed": "2024-01-02T15:37:43.329444Z",
            "media_type": "application/vnd.docker.container.image.v1+json",
            "content_type": "image",
            "digest": "sha256:c6a2222596d37028fcb292cc2f410cc47cefd255d88b1c40ee005a95e68dd53f"
        },
        {
            "creator": 6553388,
            "id": 500958191,
            "images": [
                {
                    "architecture": "amd64",
                    "features": "",
                    "variant": null,
                    "digest": "sha256:f2058fed7fa253e5764a119a6e4ed7cdaefca3e0702e3679fa010f7864367274",
                    "os": "linux",
                    "os_features": "",
                    "os_version": null,
                    "size": 754666226,
                    "status": "active",
                    "last_pulled": "2024-05-05T15:13:55.791927Z",
                    "last_pushed": "2023-08-27T05:28:20.506903Z"
                }
            ],
            "last_updated": "2023-08-27T05:28:20.627011Z",
            "last_updater": 6553388,
            "last_updater_username": "mikuras",
            "name": "0.13",
            "repository": 21775544,
            "full_size": 754666226,
            "v2": true,
            "tag_status": "active",
            "tag_last_pulled": "2024-05-05T15:13:55.791927Z",
            "tag_last_pushed": "2023-08-27T05:28:20.627011Z",
            "media_type": "application/vnd.docker.container.image.v1+json",
            "content_type": "image",
            "digest": "sha256:f2058fed7fa253e5764a119a6e4ed7cdaefca3e0702e3679fa010f7864367274"
        }
    ]
}
"""
