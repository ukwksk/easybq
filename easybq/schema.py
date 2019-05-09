# -*- coding: utf-8 -*-
import json

from google.cloud import bigquery

NULLABLE = "NULLABLE"
REQUIRED = "REQUIRED"
REPEATED = "REPEATED"


def json_file2schema(filename):
    with open(filename) as f:
        schema = json.load(f)

    return json2schema(schema)


def json2schema(schema):
    schema = [bigquery.SchemaField.from_api_repr(s) for s in schema]
    return schema
