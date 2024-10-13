import json
import avro.schema
from avro.datafile import DataFileWriter
from avro.io import DatumWriter
from typing import Generator, TextIO
import pyarrow
import pyarrow.parquet
import great_expectations as gx
import pandera.polars as pa
import polars as pl


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
        if len(buffer) >= max_size:
            # (here one could do whole/bulk buffer operations)
            # ... generate from the buffer if we've hit the max buffer size...
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
    with open(output_path, "wt") as f:
        for line in extract_flat_details(_, input_generator):
            f.write(line)


def _validate_data_in_flat_details_dump_buffer(buffer: list[dict], valid_output_lines: TextIO, invalid_output_lines: TextIO) -> list[dict]:
    schema = pa.DataFrameSchema(
        columns={
            "name": pa.Column(str),
            "gender": pa.Column(str, pa.Check.isin(["male", "female"])),
            "phone": pa.Column(str, pa.Check.str_matches(r"^(\+\d{1,2}\s)?\(?\d{3}\)?[\s.-]\d{3}[\s.-]\d{4}$")),
            "cell": pa.Column(str, pa.Check.str_matches(r"^(\+\d{1,2}\s)?\(?\d{3}\)?[\s.-]\d{3}[\s.-]\d{4}$")),
            "email": pa.Column(str, pa.Check.str_matches(r"(^[a-zA-Z0-9_.+-]+@[a-zA-Z0-9-]+\.[a-zA-Z0-9-.]+$)")),
            "city": pa.Column(str),
            "state": pa.Column(str),
            "country": pa.Column(str),
        },
        strict=True
    )

    df = pl.DataFrame(buffer)

    # get indices of bad data
    try:
        schema.validate(df, lazy=True)
        bad_indexes = set()
    except pa.errors.SchemaErrors as exc:
        # print("Schema errors and failure cases:")
        # print(exc.failure_cases)
        bad_indexes = set(exc.failure_cases["index"].to_list())
        # print(bad_indexes)

    # triage good and bad data
    for ctr, record in enumerate(buffer):
        # print((ctr, record))
        if ctr in bad_indexes:
            invalid_output_lines.write(json.dumps(record))
        else:
            valid_output_lines.write(json.dumps(record))

    return []


def validate_data_in_flat_details(
    _, input_generator: Generator, valid_output_path: str, invalid_output_path: str, max_size: int
):
    with open(valid_output_path, "wt") as valid_output_lines:
        with open(invalid_output_path, "wt") as invalid_output_lines:
            buffer = []  # list of dicts representing rows of data
            for line in input_generator:
                buffer.append(json.loads(line))
                if len(buffer) >= max_size:
                    buffer = _validate_data_in_flat_details_dump_buffer(
                        buffer, valid_output_lines, invalid_output_lines
                    )
                # valid_output_lines.write(line)
            if buffer:  # clean out last remaining elements
                buffer = _validate_data_in_flat_details_dump_buffer(
                    buffer, valid_output_lines, invalid_output_lines
                )


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
