#
# Copyright (c) 2025 Airbyte, Inc., all rights reserved.
#

import pytest
from source_kobotoolbox.source import SourceKobotoolbox, KoboAssetContents, KoboStreamSubmissionTime

BASE_URL = "https://kf.kobotoolbox.org"

config = {
    "base_url": BASE_URL,
    "username": "username",
    "password": "some_password",
    "start_time": "2023-03-15T00:00:00.000+05:30",
}

assets_response = {
    "count": 3,
    "results": [
        {"uid": "aAAA1111", "name": "Form One", "has_deployment": True},
        {"uid": "aBBB2222", "name": "Form Two (draft)", "has_deployment": False},
        {"uid": "aCCC3333", "name": "Form Three", "has_deployment": True},
    ],
}


def test_check_connection_bad_credentials(requests_mock):
    requests_mock.get(f"{BASE_URL}/api/v2/assets.json", status_code=401, json={"detail": "Invalid credentials"})
    response = SourceKobotoolbox().check_connection(logger=None, config=config)
    assert response == (False, "Something went wrong. Please check your credentials")


def test_check_connection_success(requests_mock):
    requests_mock.get(f"{BASE_URL}/api/v2/assets.json", status_code=200, json=assets_response)
    response = SourceKobotoolbox().check_connection(logger=None, config=config)
    assert response == (True, None)


def test_streams_no_token(requests_mock):
    """if no auth token can be generated, no streams are returned"""
    requests_mock.get(f"{BASE_URL}/api/v2/assets.json", status_code=200, json=assets_response)
    requests_mock.post(f"{BASE_URL}/token/?format=json", status_code=401, json={"detail": "Invalid credentials"})
    response = SourceKobotoolbox().streams(config)
    assert response == []


def test_streams(requests_mock):
    """one submissions stream per deployed form plus one asset_contents stream"""
    requests_mock.get(f"{BASE_URL}/api/v2/assets.json", status_code=200, json=assets_response)
    requests_mock.post(f"{BASE_URL}/token/?format=json", status_code=200, json={"token": "my_token_123"})
    streams = SourceKobotoolbox().streams(config)

    assert len(streams) == 3
    assert isinstance(streams[0], KoboStreamSubmissionTime)
    assert streams[0].form_id == "aAAA1111"
    assert isinstance(streams[1], KoboStreamSubmissionTime)
    assert streams[1].form_id == "aCCC3333"

    asset_contents = streams[2]
    assert isinstance(asset_contents, KoboAssetContents)
    assert asset_contents.name == "asset_contents"
    # only deployed assets
    assert asset_contents.asset_uids == ["aAAA1111", "aCCC3333"]
    assert asset_contents.auth_token == "my_token_123"


@pytest.mark.parametrize(
    "forms_config_key, expected_class_name",
    [
        ("forms_using_endtime", "KoboStreamEndTime"),
        ("forms_using_end", "KoboStreamEnd"),
    ],
)
def test_streams_cursor_selection(requests_mock, forms_config_key, expected_class_name):
    requests_mock.get(f"{BASE_URL}/api/v2/assets.json", status_code=200, json=assets_response)
    requests_mock.post(f"{BASE_URL}/token/?format=json", status_code=200, json={"token": "my_token_123"})
    streams = SourceKobotoolbox().streams({**config, forms_config_key: ["Form One"]})
    assert type(streams[0]).__name__ == expected_class_name
