#
# Copyright (c) 2023 Airbyte, Inc., all rights reserved.
#

from unittest.mock import MagicMock, Mock, patch

import pytest
from airbyte_cdk.models import SyncMode
from airbyte_cdk.sources.types import StreamSlice
from source_commcare.source import SourceCommcare
from source_commcare.source import (
    scrub_unwanted_fields_,
    ensure_single_trailing_z,
    parse_datetime_with_microseconds,
    update_case_request_params,
    mk_case_record,
    mk_form_record,
    get_english_name,
    application_questions_json_schema,
    envelope_json_schema,
    ApplicationQuestions,
    User,
    Location,
    Case,
)
from datetime import datetime

BASE_URL = "https://www.commcarehq.org/a/project_space/api/v0.5"

STREAM_ARGS = {
    "project_space": "project_space",
    "form_fields_to_exclude": [],
}


@pytest.fixture(name="config")
def config_fixture_1():
    """config fixture"""
    return {
        "api_key": "apikey",
        "app_id": "appid",
        "project_space": "project_space",
        "start_date": "2022-01-01T00:00:00Z",
        "form_fields_to_exclude": [],
        "import_all_cases": False,
        "include_archived": False,
    }


@pytest.fixture(name="config_include_archived")
def config_fixture_2():
    """include_archived=True config"""
    return {
        "api_key": "apikey",
        "app_id": "appid",
        "project_space": "project_space",
        "start_date": "2022-01-01T00:00:00Z",
        "form_fields_to_exclude": [],
        "import_all_cases": False,
        "include_archived": True,
    }


@pytest.fixture(name="config_import_all_cases")
def config_fixture_3():
    """import_all_cases=True config"""
    return {
        "api_key": "apikey",
        "app_id": "appid",
        "project_space": "project_space",
        "start_date": "2022-01-01T00:00:00Z",
        "form_fields_to_exclude": [],
        "import_all_cases": True,
        "include_archived": False,
    }


@patch("source_commcare.source.Application.read_records")
def test_check_connection_success(mock_read_records, config):
    """passing test for check_connection"""
    mock_read_records.return_value = iter(["dummy_record"])

    source = SourceCommcare()
    logger_mock = Mock()

    result = source.check_connection(logger_mock, config=config)

    assert result == (True, None)
    mock_read_records.assert_called_once()


def test_check_connection_fail(mocker, config):
    """failing test for check_connection"""
    source = SourceCommcare()
    logger_mock = MagicMock()
    excepted_outcome = " Invalid apikey, project_space or app_id : 'api_key'"
    assert source.check_connection(logger_mock, config={}) == (False, excepted_outcome)


def test_ensure_single_trailing_z():
    """tests ensure_single_trailing_z"""
    assert ensure_single_trailing_z("2022-01-01T00:00:00Z") == "2022-01-01T00:00:00Z"
    assert ensure_single_trailing_z("2022-01-01T00:00:00") == "2022-01-01T00:00:00Z"


def test_parse_datetime_with_microseconds():
    """tests parse_datetime_with_microseconds"""
    assert parse_datetime_with_microseconds("2022-01-01T00:00:00Z") == datetime(
        2022, 1, 1, 0, 0, 0
    )
    assert parse_datetime_with_microseconds("2022-01-01T00:00:00.123Z") == datetime(
        2022, 1, 1, 0, 0, 0, 123000
    )
    assert parse_datetime_with_microseconds("2022-01-01T00:00:00.123") == datetime(
        2022, 1, 1, 0, 0, 0, 123000
    )
    with pytest.raises(ValueError) as exinfo:
        parse_datetime_with_microseconds("hello")
    assert str(exinfo.value) == "Could not parse datetime string hello"


def test_include_archived_false(config):
    """tests generate_streams with include_archived as False"""
    source = SourceCommcare()
    appdata = [
        {
            "modules": [
                {
                    "forms": [
                        {
                            "xmlns": "namespace",
                            "name": {"en": "english_name"},
                        }
                    ]
                }
            ]
        }
    ]
    streams = source.generate_streams({}, config, appdata)
    assert len(streams) == 5

    # form
    assert streams[0].name == "english_name"
    assert streams[0].xmlns == "namespace"
    assert streams[0].include_archived is False

    # definition/dimension streams
    assert isinstance(streams[1], ApplicationQuestions)
    assert streams[1].name == "application_questions"
    assert streams[1].app_id == "appid"
    assert isinstance(streams[2], User)
    assert streams[2].name == "user"
    assert isinstance(streams[3], Location)
    assert streams[3].name == "location"

    # case is last and every other stream name sorts before it,
    # since airbyte syncs streams in alphabetical order
    assert isinstance(streams[4], Case)
    assert streams[4].name == "zzz_case"
    assert all(s.name < "zzz_case" for s in streams[:4])


def test_include_archived_true(config_include_archived):
    """tests generate_streams with include_archived as True"""
    source = SourceCommcare()
    appdata = [
        {
            "modules": [
                {
                    "forms": [
                        {
                            "xmlns": "namespace",
                            "name": {"en": "english_name"},
                        }
                    ]
                }
            ]
        }
    ]
    streams = source.generate_streams({}, config_include_archived, appdata)
    assert len(streams) == 5

    # form
    assert streams[0].name == "english_name"
    assert streams[0].xmlns == "namespace"
    assert streams[0].include_archived is True


def test_import_all_cases_true(config_import_all_cases):
    """tests generate_streams with import_all_cases as True"""
    source = SourceCommcare()
    appdata = [
        {
            "modules": [
                {
                    "forms": [
                        {
                            "xmlns": "namespace",
                            "name": {"en": "english_name"},
                        }
                    ]
                }
            ]
        }
    ]
    streams = source.generate_streams({}, config_import_all_cases, appdata)
    assert len(streams) == 5

    # case
    assert streams[4].import_all_cases is True


def test_scrub_unwanted_fields_1():
    """tests scrub_unwanted_fields"""
    retval = scrub_unwanted_fields_(
        {
            "_id": True,
            "name": True,
        },
        {
            "_id": "id",
            "name": "name",
            "age": 25,
        },
    )

    assert retval == {"age": 25}


def test_scrub_unwanted_fields_2():
    """tests scrub_unwanted_fields"""
    retval = scrub_unwanted_fields_(
        {
            "_id": True,
            "name": True,
        },
        {
            "_id": "id",
            "nameFirst": "name",
            "nameLast": "name",
            "nameMiddle": "name",
            "age": 25,
        },
    )
    assert retval == {"age": 25}


def test_scrub_unwanted_fields_3():
    """tests scrub_unwanted_fields"""
    retval = scrub_unwanted_fields_(
        {
            "_id": True,
            "name": True,
        },
        {
            "_id": "id",
            "subobj": {
                "nameFirst": "name",
                "nameLast": "name",
                "nameMiddle": "name",
            },
            "age": 25,
        },
    )
    assert retval == {"age": 25, "subobj": {}}


def test_update_case_request_params():
    """tests update_case_request_params"""
    assert update_case_request_params(
        {
            "indexed_on_start": "2022-01-01T00:00:00",
            "offset": "10",
        },
        {
            "format": ["json"],
            "indexed_on_start": ["2024-12-01T00:00:00.000000"],
            "order_by": ["indexed_on"],
            "limit": ["5000"],
            "offset": ["5000"],
        },
        "2022-01-01T00:00:00Z",
    ) == {
        "format": ["json"],
        "indexed_on_start": ["2024-12-01T00:00:00.000000"],
        "order_by": ["indexed_on"],
        "limit": ["5000"],
        "offset": ["5000"],
    }
    assert update_case_request_params(
        {
            "indexed_on_start": "2022-01-01T00:00:00",
            "offset": "0",
        },
        {
            "format": ["json"],
            "indexed_on_start": ["2024-12-01T00:00:00.000000"],
            "order_by": ["indexed_on"],
            "limit": ["5000"],
            "offset": ["900000"],
        },
        "2022-02-01T00:00:00Z",
    ) == {
        "format": ["json"],
        "indexed_on_start": "2022-02-01T00:00:00",
        "order_by": ["indexed_on"],
        "limit": ["5000"],
        "offset": "0",
    }


def test_mk_case_record():
    """tests mk_case_record"""
    assert mk_case_record(
        {
            "k1": "v1",
            "indexed_on": "2022-01-01T00:00:00",
            "id": "record-id",
            "xform_ids": ["form-1", "form-2", "form-3"],
        }
    ) == {
        "id": "record-id",
        "indexed_on": "2022-01-01T00:00:00Z",
        "data": {
            "k1": "v1",
            "streamname": "case",
            "indexed_on": "2022-01-01T00:00:00Z",
            "id": "record-id",
            "xform_ids": "form-1,form-2,form-3",
        },
    }


# ---------------------------------------------------------------------------
# application_questions stream
# ---------------------------------------------------------------------------

application_response = {
    "id": "appid",
    "name": "My Health App",
    "version": 42,
    "built_on": None,
    "modules": [
        {
            "unique_id": "module-1",
            "name": {"en": "Survey Module"},
            "forms": [
                {
                    "unique_id": "form-1",
                    "name": {"en": "Household Survey"},
                    "xmlns": "http://openrosa.org/formdesigner/AAAA1",
                    "questions": [
                        {
                            "label": "Do you consent?",
                            "type": "Select",
                            "tag": "select1",
                            "value": "/data/survey/consent",
                            "options": [
                                {"label": "Yes", "value": "yes"},
                                {"label": "No", "value": "no"},
                            ],
                            "required": True,
                            "relevant": None,
                            "calculate": None,
                            "repeat": None,
                            "group": None,
                        },
                        {
                            "label": "Age",
                            "type": "Int",
                            "tag": "input",
                            "value": "/data/survey/age",
                        },
                    ],
                },
                {
                    # no english name -> Unnamed_ fallback from xmlns
                    "unique_id": "form-2",
                    "name": {},
                    "xmlns": "http://openrosa.org/formdesigner/BBBB2",
                    "questions": [
                        {"label": "Notes", "type": "Text", "value": "/data/notes"},
                    ],
                },
            ],
        },
        {
            # module without questions contributes no records
            "unique_id": "module-2",
            "name": {"hin": "Hindi Module"},
            "forms": [
                {
                    "unique_id": "form-3",
                    "name": {"en": "Empty Form"},
                    "xmlns": "http://openrosa.org/formdesigner/CCCC3",
                    "questions": [],
                }
            ],
        },
    ],
}


def test_get_english_name():
    """tests get_english_name"""
    assert get_english_name({"en": " Household Survey "}) == "Household Survey"
    assert get_english_name({"hin": "Hindi Name"}) == "Hindi Name"
    assert get_english_name({}) is None
    assert get_english_name(None) is None


def test_application_questions_basics():
    """tests the static properties of the application_questions stream"""
    stream = ApplicationQuestions(app_id="appid", **STREAM_ARGS)
    assert stream.name == "application_questions"
    assert stream.primary_key == ["app_id", "form_xmlns", "question_variable"]
    assert stream.get_json_schema() == application_questions_json_schema
    assert stream.path() == "application/appid/"
    assert stream.request_params({}) == {"format": "json", "extras": "true"}
    assert stream.next_page_token(Mock()) is None


def test_application_questions_read_records(requests_mock):
    """explodes modules[] -> forms[] -> questions[] into one record per question"""
    requests_mock.get(
        f"{BASE_URL}/application/appid/", status_code=200, json=application_response
    )
    stream = ApplicationQuestions(app_id="appid", **STREAM_ARGS)
    records = list(stream.read_records(sync_mode=SyncMode.full_refresh))

    assert len(records) == 3
    for record in records:
        assert record["app_id"] == "appid"
        assert record["app_name"] == "My Health App"
        assert record["app_version"] == 42
        assert record["built_on"] is None
        assert record["module_id"] == "module-1"
        assert record["module_name"] == "Survey Module"

    assert records[0]["form_id"] == "form-1"
    assert records[0]["form_name"] == "Household Survey"
    assert records[0]["form_xmlns"] == "http://openrosa.org/formdesigner/AAAA1"
    assert records[0]["question_variable"] == "/data/survey/consent"
    assert records[0]["row_index"] == 0
    # the full question dict is preserved
    assert records[0]["question"]["label"] == "Do you consent?"
    assert records[0]["question"]["type"] == "Select"
    assert records[0]["question"]["options"] == [
        {"label": "Yes", "value": "yes"},
        {"label": "No", "value": "no"},
    ]

    assert records[1]["question_variable"] == "/data/survey/age"
    assert records[1]["row_index"] == 1

    # form without an english name falls back to the Unnamed_ convention
    assert records[2]["form_name"] == "Unnamed_BBBB2"
    assert records[2]["form_id"] == "form-2"
    assert records[2]["row_index"] == 0


def test_application_questions_empty_app(requests_mock):
    """an app without modules yields no records"""
    requests_mock.get(
        f"{BASE_URL}/application/appid/",
        status_code=200,
        json={"id": "appid", "name": "Empty App", "version": 1},
    )
    stream = ApplicationQuestions(app_id="appid", **STREAM_ARGS)
    assert list(stream.read_records(sync_mode=SyncMode.full_refresh)) == []


# ---------------------------------------------------------------------------
# user and location streams
# ---------------------------------------------------------------------------

user_page_1 = {
    "meta": {"limit": 100, "next": "?format=json&limit=100&offset=100", "offset": 0},
    "objects": [
        {
            "id": "user-guid-1",
            "username": "worker1@project.commcarehq.org",
            "first_name": "Worker",
            "last_name": "One",
            "user_data": {"role": "chw"},
        },
        {
            "id": "user-guid-2",
            "username": "worker2@project.commcarehq.org",
            "first_name": "Worker",
            "last_name": "Two",
            "user_data": {},
        },
    ],
}

user_page_2 = {
    "meta": {"limit": 100, "next": None, "offset": 100},
    "objects": [
        {
            "id": "user-guid-3",
            "username": "worker3@project.commcarehq.org",
            "first_name": "Worker",
            "last_name": "Three",
            "user_data": {},
        },
    ],
}

location_page = {
    "meta": {"limit": 100, "next": None, "offset": 0},
    "objects": [
        {
            "id": 12,
            "location_id": "loc-guid-1",
            "name": "District A",
            "site_code": "district_a",
            "location_type": "district",
            "parent": None,
        },
    ],
}


def test_user_stream_basics():
    """tests the static properties of the user stream"""
    stream = User(**STREAM_ARGS)
    assert stream.name == "user"
    assert stream.primary_key == "id"
    assert stream.get_json_schema() == envelope_json_schema
    assert stream.path() == "user"
    assert stream.request_params({}) == {"format": "json", "limit": "100"}


def test_user_stream_paginated_read(requests_mock):
    """follows meta.next across pages and emits {id, data} envelopes.

    The CDK applies a ResumableFullRefreshCursor to plain full-refresh
    streams: each read_records call reads ONE page and checkpoints the
    next-page token in the stream state, and the platform's checkpoint
    reader loops until the state says the sync is complete. This test
    emulates that loop."""
    requests_mock.get(
        f"{BASE_URL}/user",
        [
            {"status_code": 200, "json": user_page_1},
            {"status_code": 200, "json": user_page_2},
        ],
    )
    stream = User(**STREAM_ARGS)

    # page 1
    records = list(stream.read_records(sync_mode=SyncMode.full_refresh))
    state = stream.state
    assert "__ab_full_refresh_sync_complete" not in state
    assert state["offset"] == ["100"]

    # page 2, driven by the checkpointed state like the platform would
    records += list(
        stream.read_records(
            sync_mode=SyncMode.full_refresh,
            stream_slice=StreamSlice(partition={}, cursor_slice=dict(state)),
        )
    )
    assert stream.state == {"__ab_full_refresh_sync_complete": True}

    assert len(records) == 3
    assert [r["id"] for r in records] == ["user-guid-1", "user-guid-2", "user-guid-3"]
    assert records[0]["data"]["username"] == "worker1@project.commcarehq.org"
    assert set(records[0].keys()) == {"id", "data"}
    # second request carried the pagination params from meta.next
    assert requests_mock.call_count == 2
    second_request_qs = requests_mock.request_history[1].qs
    assert second_request_qs["offset"] == ["100"]
    assert second_request_qs["limit"] == ["100"]


def test_user_stream_next_page_token_end():
    """no next page when meta.next is null"""
    stream = User(**STREAM_ARGS)
    response = Mock()
    response.raise_for_status.return_value = None
    response.json.return_value = user_page_2
    assert stream.next_page_token(response) is None


def test_location_stream_basics():
    """tests the static properties of the location stream"""
    stream = Location(**STREAM_ARGS)
    assert stream.name == "location"
    assert stream.primary_key == "id"
    assert stream.get_json_schema() == envelope_json_schema
    assert stream.path() == "location"
    assert stream.request_params({}) == {"format": "json", "limit": "100"}


def test_location_stream_read(requests_mock):
    """location records use location_id (the stable guid) as the envelope id"""
    requests_mock.get(f"{BASE_URL}/location", status_code=200, json=location_page)
    stream = Location(**STREAM_ARGS)
    records = list(stream.read_records(sync_mode=SyncMode.full_refresh))

    assert len(records) == 1
    assert records[0]["id"] == "loc-guid-1"
    assert records[0]["data"]["name"] == "District A"
    assert records[0]["data"]["id"] == 12


def test_location_stream_id_fallback(requests_mock):
    """falls back to the numeric id if location_id is missing"""
    requests_mock.get(
        f"{BASE_URL}/location",
        status_code=200,
        json={
            "meta": {"next": None},
            "objects": [{"id": 99, "name": "No Guid"}],
        },
    )
    stream = Location(**STREAM_ARGS)
    records = list(stream.read_records(sync_mode=SyncMode.full_refresh))
    assert records[0]["id"] == "99"


def test_mk_form_record():
    """tests mk_form_record"""
    assert mk_form_record(
        {
            "exclude": "this field",
            "k1": "v1",
            "id": "form-id",
            "indexed_on": "2022-01-01T00:00:00",
        },
        {"exclude": True},
        "indexed_on",
    ) == {
        "id": "form-id",
        "indexed_on": "2022-01-01T00:00:00Z",
        "data": {
            "id": "form-id",
            "k1": "v1",
            "indexed_on": "2022-01-01T00:00:00Z",
        },
    }
