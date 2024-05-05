import luigi
import requests
import json
import logging
import os
from pathlib import Path
from jsonschema import validate, ValidationError
from random import choices
from datetime import datetime, UTC
from string import ascii_uppercase
import avro.schema
from avro.datafile import DataFileReader, DataFileWriter
from avro.io import DatumReader, DatumWriter
import pyarrow
import pyarrow.parquet


schema = {
    "$schema": "http://json-schema.org/draft-04/schema#",
    "type": "object",
    "properties": {
        "results": {
            "type": "array",
            "minItems": 1,
            "maxItems": 1,
            "items": [
                {
                    "type": "object",
                    "properties": {
                        "gender": {"type": "string"},
                        "name": {
                            "type": "object",
                            "properties": {
                                "title": {"type": "string"},
                                "first": {"type": "string"},
                                "last": {"type": "string"},
                            },
                            "required": ["title", "first", "last"],
                        },
                        "location": {
                            "type": "object",
                            "properties": {
                                "street": {
                                    "type": "object",
                                    "properties": {
                                        "number": {"type": "integer"},
                                        "name": {"type": "string"},
                                    },
                                    "required": ["number", "name"],
                                },
                                "city": {"type": "string"},
                                "state": {"type": "string"},
                                "country": {"type": "string"},
                                "postcode": {"type": ["string", "integer"]},
                                "coordinates": {
                                    "type": "object",
                                    "properties": {
                                        "latitude": {"type": "string"},
                                        "longitude": {"type": "string"},
                                    },
                                    "required": ["latitude", "longitude"],
                                },
                                "timezone": {
                                    "type": "object",
                                    "properties": {
                                        "offset": {"type": "string"},
                                        "description": {"type": "string"},
                                    },
                                    "required": ["offset", "description"],
                                },
                            },
                            "required": [
                                "street",
                                "city",
                                "state",
                                "country",
                                "postcode",
                                "coordinates",
                                "timezone",
                            ],
                        },
                        "email": {"type": "string"},
                        "login": {
                            "type": "object",
                            "properties": {
                                "uuid": {"type": "string"},
                                "username": {"type": "string"},
                                "password": {"type": "string"},
                                "salt": {"type": "string"},
                                "md5": {"type": "string"},
                                "sha1": {"type": "string"},
                                "sha256": {"type": "string"},
                            },
                            "required": [
                                "uuid",
                                "username",
                                "password",
                                "salt",
                                "md5",
                                "sha1",
                                "sha256",
                            ],
                        },
                        "dob": {
                            "type": "object",
                            "properties": {
                                "date": {"type": "string"},
                                "age": {"type": "integer"},
                            },
                            "required": ["date", "age"],
                        },
                        "registered": {
                            "type": "object",
                            "properties": {
                                "date": {"type": "string"},
                                "age": {"type": "integer"},
                            },
                            "required": ["date", "age"],
                        },
                        "phone": {"type": "string"},
                        "cell": {"type": "string"},
                        "id": {
                            "type": "object",
                            "properties": {
                                "name": {"type": ["string", "null"]},
                                "value": {"type": ["string", "null"]},
                            },
                            "required": ["name", "value"],
                        },
                        "picture": {
                            "type": "object",
                            "properties": {
                                "large": {"type": "string"},
                                "medium": {"type": "string"},
                                "thumbnail": {"type": "string"},
                            },
                            "required": ["large", "medium", "thumbnail"],
                        },
                        "nat": {"type": "string"},
                    },
                    "required": [
                        "gender",
                        "name",
                        "location",
                        "email",
                        "login",
                        "dob",
                        "registered",
                        "phone",
                        "cell",
                        "id",
                        "picture",
                        "nat",
                    ],
                }
            ],
        },
        "info": {
            "type": "object",
            "properties": {
                "seed": {"type": "string"},
                "results": {"type": "integer"},
                "page": {"type": "integer"},
                "version": {"type": "string"},
            },
            "required": ["seed", "results", "page", "version"],
        },
    },
    "required": ["results", "info"],
}


class DownloadRandomUsers(luigi.Task):

    def output(self):
        return luigi.LocalTarget(Path("raw") / "randomusers.txt")

    def run(self):
        with self.output().temporary_path() as temp_output_path:
            with open(temp_output_path, "wt") as f:
                for _ in range(3):
                    response = requests.get("https://randomuser.me/api/")
                    if response.ok:
                        f.write(response.text)
                        f.write("\n")


class ValidateRandomUsers(luigi.Task):

    def requires(self):
        return DownloadRandomUsers()

    def output(self):
        return luigi.LocalTarget(Path("validated") / "randomusers.txt")

    def run(self):
        logger = logging.getLogger("luigi")
        with self.input().open("r") as input_lines:
            with self.output().temporary_path() as temp_output_path:
                with open(temp_output_path, "wt") as output_lines:
                    for line in input_lines:
                        try:
                            validate(instance=json.loads(line), schema=schema)
                            output_lines.write(line)
                        except ValidationError as e:
                            logger.error(e)
                            random_stamped_str = f"{''.join(choices(ascii_uppercase, k=10))}-{datetime.now(UTC).isoformat()}"
                            os.makedirs("validation-failed", exist_ok=True)
                            filename = Path("validation-failed") / random_stamped_str
                            with open(filename, "wt") as f:
                                f.write(line)


class ExtractFlatDetails(luigi.Task):

    def requires(self):
        return ValidateRandomUsers()

    def output(self):
        return luigi.LocalTarget(Path("flattened") / "randomusers.txt")

    def run(self):
        with self.input().open("r") as input_lines:
            with self.output().temporary_path() as temp_output_path:
                with open(temp_output_path, "wt") as output_lines:
                    for line in input_lines:
                        in_row = json.loads(line)["results"][0]
                        out_row = {
                            "name": in_row["name"]["first"] + " " + in_row["name"]["last"],
                            "gender": in_row["gender"],
                            "phone": in_row["phone"],
                            "cell": in_row["cell"],
                            "email": in_row["email"],
                            "city": in_row["location"]["city"],
                            "state": in_row["location"]["state"],
                            "country": in_row["location"]["country"]
                        }
                        output_lines.write(json.dumps(out_row))
                        output_lines.write("\n")


class ToAvro(luigi.Task):

    def requires(self):
        return ExtractFlatDetails()

    def output(self):
        return luigi.LocalTarget(Path("avro") / "randomusers.avro", format=luigi.format.Nop)

    def run(self):
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
            ]
        }
        with self.input().open("r") as input_lines:
            with self.output().temporary_path() as temp_output_path:
                with open(temp_output_path, "wb") as output_lines:
                    writer = DataFileWriter(output_lines, DatumWriter(), avro.schema.parse(json.dumps(avro_schema)))
                    for line in input_lines:
                        writer.append(json.loads(line))
                    writer.close()


class ToParquet(luigi.Task):

    def requires(self):
        return ExtractFlatDetails()

    def output(self):
        return luigi.LocalTarget(Path("parquet") / "randomusers.parquet", format=luigi.format.Nop)

    def run(self):
        parquet_schema = pyarrow.schema([
            ("name", pyarrow.string()),
            ("gender", pyarrow.string()),
            ("phone", pyarrow.string()),
            ("cell", pyarrow.string()),
            ("email", pyarrow.string()),
            ("city", pyarrow.string()),
            ("state", pyarrow.string()),
            ("country", pyarrow.string()),
        ])
        with self.input().open("r") as input_lines:
            with self.output().temporary_path() as temp_output_path:
                with pyarrow.parquet.ParquetWriter(temp_output_path, schema=parquet_schema) as writer:
                    for line in input_lines:
                        # take line and dress as a dict representing a 1-elt table - this is not efficient
                        table_as_dict = {k: [v] for k, v in json.loads(line).items()}
                        table = pyarrow.Table.from_pydict(table_as_dict, schema=parquet_schema)
                        writer.write_table(table)


class AllSinks(luigi.WrapperTask):

    def requires(self):
        return [
            ToParquet(),
            ToAvro()
        ]
