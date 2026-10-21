"""Failed SQL logs preserve parameter types, request values and literal text."""

import json
import logging
import subprocess
import urllib.parse
import urllib.request

import pytest

from data_converge.sql_execution import debug
from .test_model_metric_sql import clickhouse_local


PARAMETERS = {"param_start_time": "1788915600", "param_end_time": "1788919200",
              "param_system_timezone": "Asia/Kathmandu"}


def log_sql(caplog, sql, parameters=None, config=None):
    parameters = PARAMETERS if parameters is None else parameters
    request = urllib.request.Request(
        "http://localhost/?" + urllib.parse.urlencode(parameters, doseq=True), data=sql.encode())
    caplog.clear()
    with caplog.at_level(logging.ERROR, logger=debug.__name__):
        debug.log_request(config or {}, request)
    message = caplog.records[-1].getMessage()
    return message, message.split("\nsql=", 1)[1]


def test_failed_log_expands_actual_request_values_with_their_declared_types(caplog):
    sql = ("SELECT {start_time:UInt32}, {end_time:DateTime}, "
           "toDateTime({start_time:UInt32}, {system_timezone:String})")
    message, rendered = log_sql(caplog, sql)
    assert rendered == ("SELECT CAST(1788915600 AS UInt32), CAST(1788919200 AS DateTime), "
                        "toDateTime(CAST(1788915600 AS UInt32), 'Asia/Kathmandu')")
    assert "sql_rendered=true" in message


@pytest.mark.parametrize("placeholder", ["{ start_time : UInt32 }", "{start_time:UInt32\n}",
                                          "{\nstart_time\n:\nUInt32\n}"])
def test_whitespace_inside_parameters_is_rendered(caplog, placeholder):
    message, rendered = log_sql(caplog, "SELECT " + placeholder)
    assert rendered == "SELECT CAST(1788915600 AS UInt32)"
    assert "sql_rendered=true" in message


@pytest.mark.parametrize("literal", [
    "'{start_time:UInt32}'", '"{start_time:UInt32}"', '`{start_time:UInt32}`',
    "'it\\'s {start_time:UInt32}'", "'it''s {start_time:UInt32}'",
    "-- {start_time:UInt32}\n", "// {start_time:UInt32}\n",
    "#! {start_time:UInt32}\n", "# {start_time:UInt32}\n",
    "/* outer /* nested */ {start_time:UInt32} */",
    "$sql${start_time:UInt32}$sql$", "$${start_time:UInt32}$$",
    "'unclosed {start_time:UInt32}", "/* unclosed {start_time:UInt32}",
    "$tag$unclosed {start_time:UInt32}",
])
def test_rendering_does_not_replace_placeholders_in_literal_or_comment_text(caplog, literal):
    _, rendered = log_sql(caplog, "SELECT {start_time:UInt32}; " + literal)
    assert rendered == "SELECT CAST(1788915600 AS UInt32); " + literal


@pytest.mark.parametrize("placeholder,parameters", [
    ("{start_time:UInt32}", {}),
    ("{start_time:UInt32}", {"param_start_time": ["1", "2"]}),
    ("{start_time:UInt32}", {"param_start_time": "1 OR 1"}),
    ("{start_time:UInt32}", {"param_start_time": "-1"}),
    ("{start_time:DateTime}", {"param_start_time": "4294967296"}),
    ("{start_time:Float64}", PARAMETERS),
    ("{custom:String}", {"param_custom": "private-value"}),
    ("{system_timezone:String}", {"param_system_timezone": r"Etc\tUTC"}),
    ("{system_timezone:String}", {"param_system_timezone": "Etc\nUTC"}),
    ("{/* parameter comment */ start_time:UInt32}", PARAMETERS),
])
def test_incomplete_rendering_is_explicit_and_does_not_invent_parameter_values(
        caplog, placeholder, parameters):
    message, rendered = log_sql(caplog, "SELECT " + placeholder, parameters)
    assert rendered == "SELECT " + placeholder
    assert "sql_rendered=false" in message and "unresolved_parameters=" in message
    assert "private-value" not in message


def test_rendered_string_quotes_remain_redacted_for_known_credentials(caplog):
    password = "secret'quote"
    message, _ = log_sql(caplog, "SELECT {system_timezone:String}",
                         {"param_system_timezone": password},
                         {"clickhouse": {"password": password}})
    assert "secret" not in message and "quote" not in message
    assert "[REDACTED]" in message


def test_string_parameter_single_quote_is_escaped_without_changing_its_value(clickhouse_local, caplog):
    sql = "SELECT {system_timezone:String} AS value"
    parameters = {"param_system_timezone": "invalid'zone"}
    message, rendered = log_sql(caplog, sql, parameters)
    assert "sql_rendered=true" in message
    assert execute_sql(clickhouse_local, rendered, {}) == [{"value": "invalid'zone"}]
    assert execute_sql(clickhouse_local, sql, parameters) == [{"value": "invalid'zone"}]


def execute_sql(command, sql, parameters):
    arguments = [argument for name, value in parameters.items() for argument in ("--" + name, value)]
    result = subprocess.run(command + arguments + ["--query", sql + " FORMAT JSONEachRow"],
                            capture_output=True, text=True, timeout=30)
    assert result.returncode == 0, result.stderr
    return [json.loads(line) for line in result.stdout.splitlines()]


@pytest.mark.parametrize("start,end,zone", [
    ("1788915600", "1788919200", "Asia/Kathmandu"),
    ("1793509200", "1793512800", "America/New_York"),
    ("0", "4294967295", "UTC"),
])
def test_logged_sql_matches_bound_query_values_and_types(clickhouse_local, caplog, start, end, zone):
    parameters = dict(param_start_time=start, param_end_time=end, param_system_timezone=zone)
    sql = ("SELECT { start_time : UInt32 } AS start, {end_time:DateTime\n} AS end, "
           "toTypeName(start) AS start_type, toTypeName(end) AS end_type, "
           "toDateTime({start_time:UInt32}, {system_timezone:String}) AS local_time")
    _, rendered = log_sql(caplog, sql, parameters)
    assert execute_sql(clickhouse_local, rendered, {}) == execute_sql(clickhouse_local, sql, parameters)
