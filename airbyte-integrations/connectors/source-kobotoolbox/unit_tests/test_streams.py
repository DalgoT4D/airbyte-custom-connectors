#
# Copyright (c) 2025 Airbyte, Inc., all rights reserved.
#

from unittest.mock import Mock

import json
import requests
import pytest
from source_kobotoolbox.source import KoboStreamSubmissionTime, KoboAssetContents, asset_contents_json_schema

BASE_URL = "https://kf.kobotoolbox.org"
PAGINATION_LIMIT = 30000

stream_config = {
    "config": {
        "base_url": BASE_URL,
        "username": "username",
        "password": "my_password",
        "start_time": "2023-03-15T00:00:00.000+05:30",
    },
    "form_id": "my_form_id",
    "schema": {},
    "name": "my_form",
    "pagination_limit": PAGINATION_LIMIT,
    "auth_token": "my_token_123",
}

CURSOR = "_submission_time"


@pytest.mark.parametrize('config', [(stream_config)])
def test_stream_base_url(config):
    stream = KoboStreamSubmissionTime(**config)
    assert stream.url_base == f"{BASE_URL}/api/v2/assets/{config['form_id']}/"


@pytest.mark.parametrize('config', [(stream_config)])
def test_json_schema(config):
    stream = KoboStreamSubmissionTime(**config)
    assert stream.get_json_schema() == {}


@pytest.mark.parametrize('config, next_page_token', [(stream_config, None)])
def test_request_params(config, next_page_token):
    stream = KoboStreamSubmissionTime(**config)
    assert stream.request_params({}, None, next_page_token) == {
        'start': 0,
        'limit': config['pagination_limit'],
        "sort": json.dumps({CURSOR: 1}),
        "query": json.dumps({CURSOR: {"$gte": config['config']['start_time']}})
    }


@pytest.mark.parametrize('config, total_records, params, next_page_token', [
    (
        stream_config,
        50000,
        {'start': 100, 'limit': 100},
        {'start': '200', 'limit': '100'}
    ),
    (
        stream_config,
        1729,
        {'start': 1700, 'limit': 100},
        None
    )
])
def test_next_page_token(config, params, next_page_token, total_records):
    stream = KoboStreamSubmissionTime(**config)
    response = Mock(spec=requests.Response)

    def fetch_next_page(params, total_records=total_records):
        prev = None
        next1 = None
        if params['limit'] + params['start'] < total_records:
            next1 = {'limit': params['limit'], 'start': params['limit'] + params['start']}

        if params['start'] > 0:
            prev = params

        return (prev, next1)

    def fetch_request(response, params, url, total_records=total_records):

        (prev, next1) = fetch_next_page(params)

        if prev is not None:
            prev = f"{url}?limit={prev['limit']}&start={prev['start']}"

        if next1 is not None:
            next1 = f"{url}?limit={next1['limit']}&start={next1['start']}"

        response.json.return_value = {
            "count": total_records,
            "next": next1,
            "previous": prev,
            "results": []
        }

        return response

    url = stream.url_base + stream.path()

    response = fetch_request(response, params, url)

    assert next_page_token == stream.next_page_token(response)


# ---------------------------------------------------------------------------
# asset_contents stream
# ---------------------------------------------------------------------------

asset_contents_config = {
    "config": {
        "base_url": BASE_URL,
        "username": "username",
        "password": "my_password",
        "start_time": "2023-03-15T00:00:00.000+05:30",
    },
    "asset_uids": ["aAAA1111", "aCCC3333"],
    "auth_token": "my_token_123",
}

asset_detail_response = {
    "uid": "aAAA1111",
    "name": "Form One",
    # version_id advances with every draft edit; deployment__version_id is the
    # version currently deployed, which is what submissions' __version__ points to
    "version_id": "vDRAFT999",
    "deployed_version_id": "vDEPLOYED123",
    "deployment__version_id": "vDEPLOYED123",
    "content": {
        "translations": ["English (en)", "Hindi (hi)"],
        "survey": [
            {"type": "start", "name": "start"},
            {"type": "select_one", "name": "gender", "label": ["Gender", "लिंग"], "select_from_list_name": "gender_choices"},
        ],
        "choices": [
            {"list_name": "gender_choices", "name": "female", "label": ["Female", "महिला"]},
        ],
    },
}


def test_asset_contents_basics():
    stream = KoboAssetContents(**asset_contents_config)
    assert stream.name == "asset_contents"
    assert stream.url_base == f"{BASE_URL}/api/v2/"
    assert stream.primary_key == ["asset_uid", "version_id", "sheet", "row_index"]
    assert stream.get_json_schema() == asset_contents_json_schema
    assert stream.request_headers({}) == {"Authorization": "Token my_token_123"}


def test_asset_contents_stream_slices_and_path():
    stream = KoboAssetContents(**asset_contents_config)
    slices = list(stream.stream_slices())
    assert slices == [{"asset_uid": "aAAA1111"}, {"asset_uid": "aCCC3333"}]
    assert stream.path(stream_slice=slices[0]) == "assets/aAAA1111.json"


def test_asset_contents_no_pagination():
    stream = KoboAssetContents(**asset_contents_config)
    response = Mock(spec=requests.Response)
    response.json.return_value = asset_detail_response
    assert stream.next_page_token(response) is None


def test_asset_contents_parse_response():
    stream = KoboAssetContents(**asset_contents_config)
    response = Mock(spec=requests.Response)
    response.json.return_value = asset_detail_response

    records = list(stream.parse_response(response))
    assert len(records) == 3

    survey_records = [r for r in records if r["sheet"] == "survey"]
    choices_records = [r for r in records if r["sheet"] == "choices"]
    assert len(survey_records) == 2
    assert len(choices_records) == 1

    for record in records:
        assert record["asset_uid"] == "aAAA1111"
        assert record["asset_name"] == "Form One"
        # must be the DEPLOYED version, not the draft version_id
        assert record["version_id"] == "vDEPLOYED123"
        assert record["translations"] == ["English (en)", "Hindi (hi)"]

    assert survey_records[0]["row_index"] == 0
    assert survey_records[1]["row_index"] == 1
    assert survey_records[1]["row"]["name"] == "gender"
    assert choices_records[0]["row_index"] == 0
    assert choices_records[0]["row"]["list_name"] == "gender_choices"


def test_asset_contents_version_id_fallbacks():
    stream = KoboAssetContents(**asset_contents_config)
    response = Mock(spec=requests.Response)

    # no deployment__version_id -> fall back to deployed_version_id
    asset = dict(asset_detail_response)
    asset.pop("deployment__version_id")
    asset["deployed_version_id"] = "vFALLBACK1"
    response.json.return_value = asset
    records = list(stream.parse_response(response))
    assert all(r["version_id"] == "vFALLBACK1" for r in records)

    # neither -> fall back to version_id
    asset = dict(asset_detail_response)
    asset.pop("deployment__version_id")
    asset.pop("deployed_version_id")
    response.json.return_value = asset
    records = list(stream.parse_response(response))
    assert all(r["version_id"] == "vDRAFT999" for r in records)


def test_asset_contents_empty_content():
    stream = KoboAssetContents(**asset_contents_config)
    response = Mock(spec=requests.Response)
    response.json.return_value = {"uid": "aAAA1111", "name": "Form One", "version_id": "v1", "content": {}}
    assert list(stream.parse_response(response)) == []
