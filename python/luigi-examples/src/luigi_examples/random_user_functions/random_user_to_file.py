import json
import os
from pathlib import Path
import avro.schema
from avro.datafile import DataFileReader, DataFileWriter
from avro.io import DatumReader, DatumWriter
import pyarrow
import pyarrow.parquet


def extract_flat_details(_, input_generator, output_path):
    with open(output_path, "wt") as output_lines:
        for line in input_generator:
            in_row = json.loads(line)["results"][0]
            out_row = {
                "name": in_row["name"]["first"] + " " + in_row["name"]["last"],
                "gender": in_row["gender"],
                "phone": in_row["phone"],
                "cell": in_row["cell"],
                "email": in_row["email"],
                "city": in_row["location"]["city"],
                "state": in_row["location"]["state"],
                "country": in_row["location"]["country"],
            }
            output_lines.write(json.dumps(out_row))
            output_lines.write("\n")


def to_avro(_, input_generator, output_path):
    avro_schema = {
        "namespace": "randomusers",
        "type": "record",
        "name": "user",
        "fields": [
            {"name": "name", "type": "string"},
            {"name": "gender", "type": "string"},
            {"name": "phone", "type": "string"},
            {"name": "cell", "type": "string"},
            {"name": "email", "type": "string"},
            {"name": "city", "type": "string"},
            {"name": "state", "type": "string"},
            {"name": "country", "type": "string"},
        ],
    }
    with open(output_path, "wb") as output_lines:
        writer = DataFileWriter(
            output_lines, DatumWriter(), avro.schema.parse(json.dumps(avro_schema))
        )
        for line in input_generator:
            writer.append(json.loads(line))
        writer.close()


def to_parquet(_, input_generator, output_path):
    parquet_schema = pyarrow.schema(
        [
            ("name", pyarrow.string()),
            ("gender", pyarrow.string()),
            ("phone", pyarrow.string()),
            ("cell", pyarrow.string()),
            ("email", pyarrow.string()),
            ("city", pyarrow.string()),
            ("state", pyarrow.string()),
            ("country", pyarrow.string()),
        ]
    )
    with pyarrow.parquet.ParquetWriter(output_path, schema=parquet_schema) as writer:
        for line in input_generator:
            # take line and dress as a dict representing a 1-elt table - this is not efficient
            table_as_dict = {k: [v] for k, v in json.loads(line).items()}
            table = pyarrow.Table.from_pydict(table_as_dict, schema=parquet_schema)
            writer.write_table(table)
