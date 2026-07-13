#
# Copyright (c) 2022 Airbyte, Inc., all rights reserved.
#

from abc import ABC
from datetime import datetime
from typing import Any, Iterable, List, Mapping, MutableMapping, Optional, Tuple
from urllib.parse import parse_qs

import requests
from airbyte_cdk.models import SyncMode
from airbyte_cdk.sources import AbstractSource
from airbyte_cdk.sources.streams import CheckpointMixin, Stream
from airbyte_cdk.sources.streams.http import HttpStream
from airbyte_cdk.sources.streams.http.requests_native_auth import TokenAuthenticator


def ensure_single_trailing_z(dtstr: str):
    """return the dtstr with a trailing Z, appending one if it's missing"""
    if dtstr.endswith("Z"):
        return dtstr
    return dtstr + "Z"


def parse_datetime_with_microseconds(dtstr: str):
    """parse a datetime string with or without microseconds"""
    for date_format in [
        "%Y-%m-%dT%H:%M:%S.%fZ",
        "%Y-%m-%dT%H:%M:%SZ",
        "%Y-%m-%dT%H:%M:%S.%f",
    ]:
        try:
            return datetime.strptime(dtstr, date_format)
        except ValueError:
            pass
    raise ValueError(f"Could not parse datetime string {dtstr}")


def scrub_unwanted_fields_(fields_to_exclude: dict, obj: dict[str, str]) -> dict:
    """returns the obj without unwanted fields"""
    new_dict = {}
    for key, value in obj.items():
        if key in fields_to_exclude:
            continue
        if any(key.startswith(prefix) for prefix in fields_to_exclude):
            continue
        if isinstance(value, dict):
            new_dict[key] = scrub_unwanted_fields_(fields_to_exclude, value)
        else:
            new_dict[key] = value
    return new_dict


def update_case_request_params(params: dict, next_page_token: dict, cursor_value: str):
    """updates the request params for the Case stream"""
    # next_page_token = {'format': ['json'], 'indexed_on_start': ['2024-12-01T00:00:00.000000'], 'order_by': ['indexed_on'], 'limit': ['5000'], 'offset': ['5000']}
    MAX_OFFSET = 900000
    params.update(next_page_token)
    if "offset" in next_page_token and int(next_page_token["offset"][0]) >= MAX_OFFSET:
        params["indexed_on_start"] = cursor_value.replace("Z", "")
        params["offset"] = "0"
    return params


def mk_case_record(record: dict):
    """returns a case record"""
    record["streamname"] = "case"
    record["indexed_on"] = ensure_single_trailing_z(record["indexed_on"])
    record["xform_ids"] = ",".join(record["xform_ids"])
    retval = {
        "id": record["id"],
        "indexed_on": record["indexed_on"],
        "data": record,
    }
    return retval


def get_english_name(name_dict) -> Optional[str]:
    """returns the English name from a CommCare {lang: name} dict,
    falling back to the first non-empty translation"""
    if isinstance(name_dict, dict):
        if name_dict.get("en"):
            return str(name_dict["en"]).strip()
        for value in name_dict.values():
            if value:
                return str(value).strip()
    return None


# schema for the application_questions stream: one record per question in the
# application structure (modules[] -> forms[] -> questions[])
application_questions_json_schema = {
    "$schema": "http://json-schema.org/draft-07/schema#",
    "type": "object",
    "additionalProperties": True,
    "properties": {
        "app_id": {"type": ["string", "null"]},
        "app_name": {"type": ["string", "null"]},
        "app_version": {"type": ["number", "null"]},
        "built_on": {"type": ["string", "null"]},
        "module_id": {"type": ["string", "null"]},
        "module_name": {"type": ["string", "null"]},
        "form_id": {"type": ["string", "null"]},
        "form_name": {"type": ["string", "null"]},
        "form_xmlns": {"type": ["string", "null"]},
        "question_variable": {"type": ["string", "null"]},
        "row_index": {"type": ["number", "null"]},
        "question": {"type": "object"},
    },
}

# schema for the {id, data} record envelope used by the user and location
# streams (same envelope as the form streams, minus the cursor field)
envelope_json_schema = {
    "$schema": "http://json-schema.org/draft-07/schema#",
    "type": "object",
    "properties": {
        "id": {"type": "string"},
        "data": {"type": "object"},
    },
}


def mk_form_record(record: dict, fields_to_exclude: dict, cursor_field: str):
    """returns a form record"""
    newform = scrub_unwanted_fields_(fields_to_exclude, record)
    newform[cursor_field] = ensure_single_trailing_z(newform[cursor_field])
    retval = {
        "id": newform["id"],
        "data": newform,
    }
    retval[cursor_field] = newform[cursor_field]
    return retval


class CommcareStream(HttpStream, ABC):
    """Basic full refresh stream"""

    def __init__(self, project_space, form_fields_to_exclude, **kwargs):
        super().__init__(**kwargs)
        self.project_space = project_space
        self.form_fields_to_exclude = form_fields_to_exclude

    @property
    def url_base(self) -> str:
        return f"https://www.commcarehq.org/a/{self.project_space}/api/v0.5/"

    # These class variables save state
    # forms holds form ids and we filter cases which contain one of these form ids
    # last_form_date stores the date of the last form read so the next cycle for forms and cases starts at the same timestamp
    forms = set()
    last_form_date = None
    schemas = {}

    @property
    def dateformat_for_query(self) -> str:
        """return the date format for query parameters"""
        return "%Y-%m-%dT%H:%M:%S.%f"

    def scrub_unwanted_fields(self, form: dict[str, str]) -> dict:
        """removes unwanted fields from the form"""
        return scrub_unwanted_fields_(self.form_fields_to_exclude, form)

    def next_page_token(
        self, response: requests.Response
    ) -> Optional[Mapping[str, Any]]:
        try:
            # Server returns status 500 when there are no more rows.
            # raise an error if server returns an error
            response.raise_for_status()
            meta = response.json()["meta"]
            return parse_qs(meta["next"][1:])
        except Exception as ex:
            return ex

    def request_params(
        self,
        stream_state: Mapping[str, Any],
        stream_slice: Mapping[str, any] = None,
        next_page_token: Mapping[str, Any] = None,
    ) -> MutableMapping[str, Any]:
        params = {"format": "json"}
        return params


class Application(CommcareStream):
    primary_key = "id"

    def __init__(self, app_id, **kwargs):
        super().__init__(**kwargs)
        self.app_id = app_id

    def path(
        self,
        stream_state: Mapping[str, Any] = None,
        stream_slice: Mapping[str, Any] = None,
        next_page_token: Mapping[str, Any] = None,
    ) -> str:
        return f"application/{self.app_id}/"

    def next_page_token(
        self, response: requests.Response
    ) -> Optional[Mapping[str, Any]]:
        return None

    def request_params(
        self,
        stream_state: Mapping[str, Any],
        stream_slice: Mapping[str, any] = None,
        next_page_token: Mapping[str, Any] = None,
    ) -> MutableMapping[str, Any]:
        params = {"format": "json", "extras": "true"}
        return params

    def parse_response(
        self, response: requests.Response, **kwargs
    ) -> Iterable[Mapping]:
        yield response.json()


class ApplicationQuestions(Application):
    """
    Definition stream (full refresh / overwrite): explodes the application
    structure returned by
    https://www.commcarehq.org/a/[domain]/api/[version]/application/[app_id]/
    (modules[] -> forms[] -> questions[]) into one record per question, so
    downstream models (e.g. dbt_taf_metrics QuestionsDB) can join question
    labels/types/options to melted form submissions.

    Note on versions/builds: this endpoint returns the CURRENT structure of
    the application document identified by app_id. For the canonical app id
    (what the connector config carries) "version" is the app's current version
    counter and "built_on" is null; released builds are separate application
    documents that are not enumerable from this response. v1 therefore emits
    the current structure only, stamped with app_version/built_on.
    """

    primary_key = ["app_id", "form_xmlns", "question_variable"]

    @property
    def name(self):
        # sorts before "zzz_case" so the alphabetical-ordering trick that
        # makes the case stream sync last still holds (ordering does not
        # actually matter for this stream: it is an independent full-refresh
        # pull with no dependency on the form/case cycle)
        return "application_questions"

    def get_json_schema(self):
        return application_questions_json_schema

    def parse_response(
        self, response: requests.Response, **kwargs
    ) -> Iterable[Mapping]:
        app = response.json()
        app_id = app.get("id") or self.app_id
        app_name = app.get("name")
        app_version = app.get("version")
        built_on = app.get("built_on")
        for module in app.get("modules") or []:
            module_id = module.get("unique_id")
            module_name = get_english_name(module.get("name"))
            for form in module.get("forms") or []:
                xmlns = form.get("xmlns")
                form_name = get_english_name(form.get("name"))
                if not form_name and xmlns:
                    # same fallback convention as the generated form streams
                    form_name = f"Unnamed_{xmlns[-5:]}"
                for row_index, question in enumerate(form.get("questions") or []):
                    yield {
                        "app_id": app_id,
                        "app_name": app_name,
                        "app_version": app_version,
                        "built_on": built_on,
                        "module_id": module_id,
                        "module_name": module_name,
                        "form_id": form.get("unique_id"),
                        "form_name": form_name,
                        "form_xmlns": xmlns,
                        "question_variable": question.get("value"),
                        "row_index": row_index,
                        "question": question,
                    }


class PaginatedEnvelopeStream(CommcareStream):
    """
    Full-refresh stream over a paginated (meta.next) CommCare list endpoint,
    emitting records in the connector's standard {id, data} envelope.
    Meant to be synced with destination mode "overwrite".
    """

    primary_key = "id"
    # field of the API object used as the envelope id
    id_field = "id"
    limit = 100

    def get_json_schema(self):
        return envelope_json_schema

    def next_page_token(
        self, response: requests.Response
    ) -> Optional[Mapping[str, Any]]:
        try:
            response.raise_for_status()
            meta = response.json()["meta"]
            if meta.get("next"):
                return parse_qs(meta["next"][1:])
            return None
        except Exception:
            return None

    def request_params(
        self,
        stream_state: Mapping[str, Any],
        stream_slice: Mapping[str, any] = None,
        next_page_token: Mapping[str, Any] = None,
    ) -> MutableMapping[str, Any]:
        params = {"format": "json", "limit": str(self.limit)}
        if next_page_token:
            params.update(next_page_token)
        return params

    def parse_response(
        self, response: requests.Response, **kwargs
    ) -> Iterable[Mapping]:
        for record in response.json()["objects"]:
            record_id = record.get(self.id_field) or record.get("id")
            yield {"id": str(record_id), "data": record}


class User(PaginatedEnvelopeStream):
    """
    Mobile workers, one record per user.
    docs: https://www.commcarehq.org/a/[domain]/api/[version]/user/
    """

    @property
    def name(self):
        return "user"

    def path(
        self,
        stream_state: Mapping[str, Any] = None,
        stream_slice: Mapping[str, Any] = None,
        next_page_token: Mapping[str, Any] = None,
    ) -> str:
        return "user"


class Location(PaginatedEnvelopeStream):
    """
    Locations, one record per location.
    docs: https://www.commcarehq.org/a/[domain]/api/[version]/location/
    """

    # location objects carry both a numeric "id" and a guid "location_id";
    # location_id is the stable join key used by case/form data
    id_field = "location_id"

    @property
    def name(self):
        return "location"

    def path(
        self,
        stream_state: Mapping[str, Any] = None,
        stream_slice: Mapping[str, Any] = None,
        next_page_token: Mapping[str, Any] = None,
    ) -> str:
        return "location"


class IncrementalStream(CommcareStream, CheckpointMixin):
    cursor_field = "indexed_on"
    _cursor_value = None

    @property
    def state(self) -> Mapping[str, Any]:
        return {self.cursor_field: self._cursor_value}

    @state.setter
    def state(self, value: Mapping[str, Any]):
        if self.cursor_field in value:
            self._cursor_value = parse_datetime_with_microseconds(
                value[self.cursor_field]
            )

    @property
    def sync_mode(self):
        return SyncMode.incremental

    @property
    def supported_sync_modes(self):
        return [SyncMode.incremental]

    def next_page_token(
        self, response: requests.Response
    ) -> Optional[Mapping[str, Any]]:
        try:
            # Server returns status 500 when there are no more rows.
            # raise an error if server returns an error
            response.raise_for_status()
            meta = response.json()["meta"]
            if meta["next"]:
                return parse_qs(meta["next"][1:])
            return None
        except Exception:
            return None

    def request_params(
        self,
        stream_state: Mapping[str, Any],
        stream_slice: Mapping[str, any] = None,
        next_page_token: Mapping[str, Any] = None,
    ) -> MutableMapping[str, Any]:
        params = {"format": "json"}
        if next_page_token:
            params.update(next_page_token)
        return params

    def parse_response(
        self, response: requests.Response, **kwargs
    ) -> Iterable[Mapping]:
        for o in iter(response.json()["objects"]):
            yield o
        return None


class Case(IncrementalStream):
    """
    docs: https://www.commcarehq.org/a/[domain]/api/[version]/case/
    """

    cursor_field = "indexed_on"
    primary_key = "id"

    def __init__(self, start_date, schema, app_id, import_all_cases, **kwargs):
        super().__init__(**kwargs)
        self._cursor_value = parse_datetime_with_microseconds(start_date)
        self.schema = schema
        self.last_record = None
        self.import_all_cases = import_all_cases

    def get_json_schema(self):
        return self.schema

    @property
    def name(self):
        # Airbyte orders streams in alpha order but since we have dependent peers and we need to
        # pull all forms before cases, we name this stream to
        # ensure this stream gets pulled last (assuming ascii stream names only)
        return "zzz_case"

    def path(
        self,
        stream_state: Mapping[str, Any] = None,
        stream_slice: Mapping[str, Any] = None,
        next_page_token: Mapping[str, Any] = None,
    ) -> str:
        return "case"

    def request_params(
        self,
        stream_state: Mapping[str, Any],
        stream_slice: Mapping[str, any] = None,
        next_page_token: Mapping[str, Any] = None,
    ) -> MutableMapping[str, Any]:
        # start date is what we saved for forms
        # if self.cursor_field in self.state else (CommcareStream.last_form_date or self.initial_date)
        ix: datetime = self.state[self.cursor_field]
        LIMIT = 5000
        params = {
            "format": "json",
            "indexed_on_start": ix.strftime(self.dateformat_for_query),
            "order_by": "indexed_on",
            "limit": str(LIMIT),
        }
        if next_page_token:
            params = update_case_request_params(
                params, next_page_token, self.last_record[self.cursor_field]
            )
        return params

    def read_records(self, *args, **kwargs) -> Iterable[Mapping[str, Any]]:
        for record in super().read_records(*args, **kwargs):
            self.last_record = record
            if self.import_all_cases or any(
                f in CommcareStream.forms for f in record["xform_ids"]
            ):
                self._cursor_value = parse_datetime_with_microseconds(
                    record[self.cursor_field]
                )
                retval = mk_case_record(record)
                yield retval
        if self._cursor_value.microsecond == 0:
            # Airbyte converts the cursor_field value (datetime) to string when it saves the state and
            # our state setter parses the saved state with a format that contains microseconds
            # self._cursor_value must have non-zero microseconds for the formatting and parsing to work correctly.
            # This issue would also occur if an incoming record had a timestamp with zero microseconds
            self._cursor_value = self._cursor_value.replace(microsecond=10)
        # This cycle of pull is complete so clear out the form ids we saved for this cycle
        CommcareStream.forms.clear()


class Form(IncrementalStream):
    """
    docs: https://www.commcarehq.org/a/[domain]/api/[version]/form/
    """

    cursor_field = "indexed_on"
    primary_key = "id"

    def __init__(
        self, start_date, app_id, name, xmlns, schema, include_archived, **kwargs
    ):
        super().__init__(**kwargs)
        self.app_id = app_id
        self._cursor_value = parse_datetime_with_microseconds(start_date)
        self.streamname = name
        self.xmlns = xmlns
        self.schema = schema
        self.include_archived = include_archived

    @property
    def name(self):
        return getattr(self, "streamname", "form")

    def get_json_schema(self):
        return self.schema

    def path(
        self,
        stream_state: Mapping[str, Any] = None,
        stream_slice: Mapping[str, Any] = None,
        next_page_token: Mapping[str, Any] = None,
    ) -> str:
        return "form"

    def request_params(
        self,
        stream_state: Mapping[str, Any],
        stream_slice: Mapping[str, any] = None,
        next_page_token: Mapping[str, Any] = None,
    ) -> MutableMapping[str, Any]:
        # if self.cursor_field in self.state else self.initial_date
        ix: datetime = self.state[self.cursor_field]
        params = {
            "format": "json",
            "app_id": self.app_id,
            "indexed_on_start": ix.strftime(self.dateformat_for_query),
            "order_by": "indexed_on",
            "limit": "1000",
            "include_archived": self.include_archived,
            "xmlns": self.xmlns,
        }
        if next_page_token:
            params.update(next_page_token)
        return params

    def read_records(self, *args, **kwargs) -> Iterable[Mapping[str, Any]]:
        for record in super().read_records(*args, **kwargs):
            self._cursor_value = parse_datetime_with_microseconds(
                record[self.cursor_field]
            )
            CommcareStream.forms.add(record["id"])
            retval = mk_form_record(
                record, self.form_fields_to_exclude, self.cursor_field
            )
            yield retval
        if self._cursor_value.microsecond == 0:
            # Airbyte converts the cursor_field value (datetime) to string when it saves the state and
            # our state setter parses the saved state with a format that contains microseconds
            # self._cursor_value must have non-zero microseconds for the formatting and parsing to work correctly.
            # This issue would also occur if an incoming record had a timestamp with zero microseconds
            self._cursor_value = self._cursor_value.replace(microsecond=10)


# Source
class SourceCommcare(AbstractSource):
    def check_connection(self, logger, config) -> Tuple[bool, any]:
        try:
            auth = TokenAuthenticator(config["api_key"], auth_method="ApiKey")
            args = {
                "authenticator": auth,
            }
            form_fields_to_exclude = config.get("form_fields_to_exclude", [])
            next(
                Application(
                    **{
                        **args,
                        "app_id": config["app_id"],
                        "form_fields_to_exclude": form_fields_to_exclude,
                        "project_space": config["project_space"],
                    }
                ).read_records(SyncMode.full_refresh)
            )
            return True, None
        except Exception as error:
            return False, " Invalid apikey, project_space or app_id : " + str(error)

    def base_schema(self):
        return {
            "$schema": "http://json-schema.org/draft-07/schema#",
            "type": "object",
            "properties": {
                "id": {"type": "string"},
                "indexed_on": {
                    "type": "string",
                    "format": "date-time",
                    "airbyte_type": "timestamp_with_timezone",
                },
                "data": {"type": "object"},
            },
        }

    def streams(self, config: Mapping[str, Any]) -> List[Stream]:
        auth = TokenAuthenticator(config["api_key"], auth_method="ApiKey")
        args = {
            "authenticator": auth,
        }
        form_fields_to_exclude = config.get("form_fields_to_exclude", [])
        appdata = Application(
            **{
                **args,
                "app_id": config["app_id"],
                "form_fields_to_exclude": form_fields_to_exclude,
                "project_space": config["project_space"],
            }
        ).read_records(sync_mode=SyncMode.full_refresh)

        # Generate streams for forms, one per xmlns and one stream for cases.
        streams = self.generate_streams(args, config, appdata)
        return streams

    def generate_streams(self, args, config, appdata):
        form_fields_to_exclude = config.get("form_fields_to_exclude", [])
        form_args = {
            "app_id": config["app_id"],
            "start_date": config["start_date"],
            "form_fields_to_exclude": form_fields_to_exclude,
            "project_space": config["project_space"],
            **args,
        }
        streams = []
        name2xmlns = {}

        # Collect the form names and xmlns from the application
        for record in appdata:
            mods = record["modules"]
            for m in mods:
                forms = m["forms"]
                for f in forms:
                    xmlns = f["xmlns"]
                    formname = ""
                    if "en" in f["name"]:
                        formname = f["name"]["en"].strip()
                    else:
                        # Unknown forms are named UNNAMED_xxxxx where xxxxx are the last 5 digits of the XMLNS
                        # This convention gives us repeatable names
                        formname = f"Unnamed_{xmlns[-5:]}"

                    name = formname
                    name2xmlns[name] = xmlns

        # Create the streams from the collected names
        # Sorted by name
        for k in sorted(name2xmlns):
            key = name2xmlns[k]
            stream = Form(
                name=k,
                xmlns=key,
                schema=self.base_schema(),
                include_archived=config.get("include_archived", False),
                **form_args,
            )
            streams.append(stream)

        # Definition/dimension streams (full refresh / overwrite), used by
        # dbt_taf_metrics: application_questions feeds the QuestionsDB
        # label/type joins, user and location feed the survey_workers and
        # locations dims. Their names all sort before "zzz_case", so the
        # alphabetical-ordering trick that makes the case stream sync last
        # is preserved (and none of them participate in the form/case
        # dependency anyway).
        common_args = {
            "project_space": config["project_space"],
            "form_fields_to_exclude": form_fields_to_exclude,
            **args,
        }
        streams.append(ApplicationQuestions(app_id=config["app_id"], **common_args))
        streams.append(User(**common_args))
        streams.append(Location(**common_args))

        stream = Case(
            start_date=config["start_date"],
            schema=self.base_schema(),
            app_id=config["app_id"],
            import_all_cases=config.get("import_all_cases", False),
            project_space=config["project_space"],
            form_fields_to_exclude=form_fields_to_exclude,
            **args,
        )

        streams.append(stream)

        return streams
