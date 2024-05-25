import json
import avro.schema
from avro.datafile import DataFileWriter
from avro.io import DatumWriter
from typing import Generator
import pyarrow
import pyarrow.parquet


def extract_flat_details_bulk(_, input_generator: Generator[str, None, None], max_size: int) -> Generator[str, None, None]:
    """ As an example, this performs the same job as extract_flat_details, but with a internal buffer.
        This example covers the use case where bulk operations over multiple rows would need to be performed.
    """
    buffer = []
    for line in input_generator:
        # fill up the buffer...
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
        buffer.append(json.dumps(out_row) + "\n")
        # ... generate from the buffer if we've hit the max buffer size...
        if len(buffer) >= max_size:
            yield from buffer
            buffer = []
    # ... generate the last few elements from input_generator that didn't quite make a full buffer
    yield from buffer


def extract_flat_details(_, input_generator: Generator[str, None, None]) -> Generator[str, None, None]:
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
        yield json.dumps(out_row) + "\n"


def extract_flat_details_to_file(_, input_generator: Generator[str, None, None], output_path: str):
    with open(output_path, "wt") as output_lines:
        for line in extract_flat_details(_, input_generator):
            output_lines.write(line)


def to_avro_file(_, input_generator: Generator[str, None, None], output_path: str):
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


def to_parquet_file(_, input_generator: Generator[str, None, None], output_path: str):
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
            # take line and dress as a 1-elt table
            # this is not efficient
            pylist = [json.loads(line)]
            table = pyarrow.Table.from_pylist(pylist, schema=parquet_schema)
            writer.write_table(table)
