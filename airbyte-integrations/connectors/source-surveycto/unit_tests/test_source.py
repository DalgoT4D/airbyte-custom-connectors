#
# Copyright (c) 2025 Airbyte, Inc., all rights reserved.
#

from unittest.mock import MagicMock

import pytest
from source_surveycto.source import SourceSurveycto, SurveyctoFormDefinitions, SurveyctoStream

BASE_URL = "https://server_name.surveycto.com"

submission_record = {
    "KEY": "uuid:abc-123",
    "SubmissionDate": "Feb 07, 2023 10:00:00 AM",
    "endtime": "Feb 07, 2023 09:59:00 AM",
    "some_question": "some_answer",
}


@pytest.fixture(name="config")
def config_fixture():
    return {
        "server_name": "server_name",
        "form_id": ["form_id_1", "form_id_2"],
        "start_date": "Jan 09, 2022 00:00:00 AM",
        "password": "password",
        "username": "username",
    }


@pytest.fixture(name="source")
def source_fixture():
    return SourceSurveycto()


def test_check_connection_valid(requests_mock, source, config):
    requests_mock.get(f"{BASE_URL}/api/v2/forms/data/wide/json/form_id_1", json=[submission_record])
    requests_mock.get(f"{BASE_URL}/api/v2/forms/data/wide/json/form_id_2", json=[submission_record])
    assert source.check_connection(MagicMock(), config) == (True, None)


def test_check_connection_failure(requests_mock, source, config):
    requests_mock.get(f"{BASE_URL}/api/v2/forms/data/wide/json/form_id_1", status_code=401, json={"error": "Unauthorized"})
    success, message = source.check_connection(MagicMock(), config)
    assert success is False
    assert message.startswith("Unable to connect")


def test_generate_streams(source, config):
    streams = source.generate_streams(config)
    assert len(streams) == 3
    assert all(isinstance(stream, SurveyctoStream) for stream in streams[:2])
    assert [stream.form_id for stream in streams[:2]] == ["form_id_1", "form_id_2"]
    assert isinstance(streams[2], SurveyctoFormDefinitions)


def test_streams(source, config):
    """one submissions stream per configured form plus one form_definitions stream"""
    streams = source.streams(config)
    assert len(streams) == 3

    form_definitions = streams[2]
    assert isinstance(form_definitions, SurveyctoFormDefinitions)
    assert form_definitions.name == "form_definitions"
    assert form_definitions.form_ids == ["form_id_1", "form_id_2"]


def test_streams_no_forms(source, config):
    """no form_definitions stream when no forms are configured"""
    streams = source.streams({**config, "form_id": []})
    assert streams == []
