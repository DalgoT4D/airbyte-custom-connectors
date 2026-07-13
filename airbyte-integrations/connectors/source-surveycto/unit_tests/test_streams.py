#
# Copyright (c) 2025 Airbyte, Inc., all rights reserved.
#

import io
from unittest.mock import Mock

import openpyxl
import pytest
import requests
from airbyte_cdk.models import SyncMode
from source_surveycto.source import (
    SurveyctoFormDefinitions,
    SurveyctoStream,
    form_definitions_json_schema,
    stream_json_schema,
)

BASE_URL = "https://server_name.surveycto.com"

config = {
    "server_name": "server_name",
    "form_id": ["form_id_1", "form_id_2"],
    "start_date": "Jan 09, 2022 00:00:00 AM",
    "password": "password",
    "username": "username",
}


# ---------------------------------------------------------------------------
# submissions streams
# ---------------------------------------------------------------------------


def test_survey_stream_basics():
    stream = SurveyctoStream(config=config, form_id="form_id_1", schema=stream_json_schema)
    assert stream.name == "form_id_1"
    assert stream.url_base == f"{BASE_URL}/api/v2/forms/data/wide/json/"
    assert stream.path() == "form_id_1"
    assert stream.primary_key == "KEY"
    assert stream.cursor_field == "SubmissionDate"
    assert stream.get_json_schema() == stream_json_schema
    assert stream.request_headers({}) == {"Authorization": "Basic dXNlcm5hbWU6cGFzc3dvcmQ="}
    # before any record is read, the state falls back to the configured start_date
    assert stream.request_params({}) == {"date": "Jan 09, 2022 00:00:00 AM"}
    assert stream.next_page_token(Mock(spec=requests.Response)) is None


def test_survey_stream_parse_response():
    stream = SurveyctoStream(config=config, form_id="form_id_1", schema=stream_json_schema)
    response = Mock(spec=requests.Response)
    response.json.return_value = [
        {
            "KEY": "uuid:abc-123",
            "SubmissionDate": "Feb 07, 2023 10:00:00 AM",
            "endtime": "Feb 07, 2023 09:59:00 AM",
            "some_question": "some_answer",
        }
    ]
    records = list(stream.parse_response(response, stream_state={}))
    assert len(records) == 1
    assert records[0]["KEY"] == "uuid:abc-123"
    assert records[0]["SubmissionDate"] == "Feb 07, 2023 10:00:00 AM"
    assert records[0]["endtime"] == "Feb 07, 2023 09:59:00 AM"
    assert records[0]["data"]["some_question"] == "some_answer"


# ---------------------------------------------------------------------------
# form_definitions stream
# ---------------------------------------------------------------------------


def build_xlsform(survey_rows=None, choices_rows=None, settings_rows=None, survey_header=None) -> bytes:
    """build an in-memory XLSForm .xlsx with the three standard sheets"""
    workbook = openpyxl.Workbook()
    workbook.remove(workbook.active)

    sheets = {
        "survey": [survey_header or ["type", "name", "label::English (en)", None, "Unnamed: 4"]] + (survey_rows or []),
        "choices": [["list_name", "name", "label"]] + (choices_rows or []),
        "settings": [["form_title", "form_id", "version"]] + (settings_rows or []),
    }
    for sheet_name, rows in sheets.items():
        worksheet = workbook.create_sheet(sheet_name)
        for row in rows:
            worksheet.append(row)

    buffer = io.BytesIO()
    workbook.save(buffer)
    return buffer.getvalue()


files_response = {
    "deployedGroupFiles": {
        "definitionFile": {
            "formVersion": "2222",
            "filename": "my_form_v2.xlsx",
            "downloadLink": f"{BASE_URL}/download/my_form_v2.xlsx",
        }
    },
    "previousDefinitionFiles": [
        {
            "formVersion": "1111",
            "filename": "my_form_v1.xlsx",
            "downloadLink": f"{BASE_URL}/download/my_form_v1.xlsx",
        }
    ],
}


@pytest.fixture(name="stream")
def stream_fixture():
    return SurveyctoFormDefinitions(config=config, form_ids=["form_id_1", "form_id_2"])


def read_slice(stream, form_id):
    return list(stream.read_records(sync_mode=SyncMode.full_refresh, stream_slice={"form_id": form_id}))


def test_form_definitions_basics(stream):
    assert stream.name == "form_definitions"
    assert stream.url_base == f"{BASE_URL}/"
    assert stream.primary_key == ["form_id", "form_version", "sheet", "row_index"]
    assert stream.get_json_schema() == form_definitions_json_schema
    assert stream.request_headers({}) == {"Authorization": "Basic dXNlcm5hbWU6cGFzc3dvcmQ="}
    # the files endpoint requires a unix-milliseconds timestamp param
    assert stream.request_params({}).keys() == {"t"}
    assert stream.next_page_token(Mock(spec=requests.Response)) is None


def test_form_definitions_stream_slices_and_path(stream):
    slices = list(stream.stream_slices())
    assert slices == [{"form_id": "form_id_1"}, {"form_id": "form_id_2"}]
    assert stream.path(stream_slice=slices[0]) == "forms/form_id_1/files"


def test_form_definitions_parse_response(requests_mock, stream):
    """current and previous definition-file versions are downloaded and melted
    into one record per sheet row, with cleaned column names"""
    xlsx_v2 = build_xlsform(
        survey_rows=[
            ["start", "start", None, "ignored", "ignored"],
            ["select_one gender", "gender", "Gender", None, None],
        ],
        choices_rows=[["gender", "female", "Female"]],
        settings_rows=[["My Form", "form_id_1", 2222]],
    )
    xlsx_v1 = build_xlsform(survey_rows=[["text", "name", "Name", None, None]])

    requests_mock.get(f"{BASE_URL}/forms/form_id_1/files", json=files_response)
    requests_mock.get(f"{BASE_URL}/download/my_form_v2.xlsx", content=xlsx_v2)
    requests_mock.get(f"{BASE_URL}/download/my_form_v1.xlsx", content=xlsx_v1)

    records = read_slice(stream, "form_id_1")

    assert all(record["form_id"] == "form_id_1" for record in records)
    assert {record["form_version"] for record in records} == {"2222", "1111"}

    v2_records = [record for record in records if record["form_version"] == "2222"]
    assert [(record["sheet"], record["row_index"]) for record in v2_records] == [
        ("survey", 0),
        ("survey", 1),
        ("choices", 0),
        ("settings", 0),
    ]

    gender_row = v2_records[1]["row"]
    # column names are cleaned: lowercased, [ ,;{}()\n\t=-] replaced with '_';
    # None and 'unnamed' columns are dropped
    assert gender_row == {"type": "select_one gender", "name": "gender", "label::english__en_": "Gender"}

    # settings values are stringified
    assert v2_records[3]["row"]["version"] == "2222"

    v1_records = [record for record in records if record["form_version"] == "1111"]
    assert [(record["sheet"], record["row_index"]) for record in v1_records] == [("survey", 0)]
    assert v1_records[0]["row"]["name"] == "name"


def test_form_definitions_blank_rows_keep_their_slot(requests_mock, stream):
    """blank rows are skipped but later rows keep their original position, so
    question order is preserved downstream"""
    xlsx = build_xlsform(
        survey_rows=[
            ["text", "q1", "Question 1", None, None],
            [None, None, None, None, None],
            ["text", "q2", "Question 2", None, None],
        ]
    )
    requests_mock.get(
        f"{BASE_URL}/forms/form_id_1/files",
        json={"deployedGroupFiles": {"definitionFile": files_response["deployedGroupFiles"]["definitionFile"]}},
    )
    requests_mock.get(f"{BASE_URL}/download/my_form_v2.xlsx", content=xlsx)

    records = [record for record in read_slice(stream, "form_id_1") if record["sheet"] == "survey"]
    assert [(record["row_index"], record["row"]["name"]) for record in records] == [(0, "q1"), (2, "q2")]


def test_form_definitions_duplicate_versions_deduped(requests_mock, stream):
    """a previous file with the same formVersion as the deployed one is not
    downloaded twice (primary key would collide)"""
    duplicate = dict(files_response["deployedGroupFiles"]["definitionFile"])
    requests_mock.get(
        f"{BASE_URL}/forms/form_id_1/files",
        json={"deployedGroupFiles": {"definitionFile": duplicate}, "previousDefinitionFiles": [duplicate]},
    )
    download = requests_mock.get(f"{BASE_URL}/download/my_form_v2.xlsx", content=build_xlsform())

    read_slice(stream, "form_id_1")
    assert download.call_count == 1


def test_form_definitions_download_failure_skips_file(requests_mock, stream):
    """a definition file that fails to download is skipped with a warning
    instead of failing the sync"""
    requests_mock.get(f"{BASE_URL}/forms/form_id_1/files", json=files_response)
    requests_mock.get(f"{BASE_URL}/download/my_form_v2.xlsx", status_code=500)
    requests_mock.get(
        f"{BASE_URL}/download/my_form_v1.xlsx",
        content=build_xlsform(survey_rows=[["text", "name", "Name", None, None]]),
    )

    records = read_slice(stream, "form_id_1")
    assert {record["form_version"] for record in records} == {"1111"}


def test_form_definitions_xls_files_skipped(requests_mock, stream):
    """legacy .xls definition files are skipped with a warning"""
    requests_mock.get(
        f"{BASE_URL}/forms/form_id_1/files",
        json={
            "deployedGroupFiles": {
                "definitionFile": {
                    "formVersion": "1111",
                    "filename": "legacy.xls",
                    "downloadLink": f"{BASE_URL}/download/legacy.xls",
                }
            }
        },
    )
    assert read_slice(stream, "form_id_1") == []


def test_form_definitions_no_definition_files(requests_mock, stream):
    requests_mock.get(f"{BASE_URL}/forms/form_id_1/files", json={"deployedGroupFiles": {}})
    assert read_slice(stream, "form_id_1") == []
