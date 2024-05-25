import logging
import json
from pathlib import Path
from avro.datafile import DataFileReader
from avro.io import DatumReader
import pyarrow.parquet
from luigi_examples.random_user_functions.random_user_to_file import (
    extract_flat_details_to_file,
    to_avro_file,
    to_parquet_file,
)


expected_flat_records = [
    {
        "name": "Damanja Hoffer",
        "gender": "female",
        "phone": "(038) 8294795",
        "cell": "(06) 54234226",
        "email": "damanja.hoffer@example.com",
        "city": "Peins",
        "state": "Overijssel",
        "country": "Netherlands",
    },
    {
        "name": "Paulina Parra",
        "gender": "female",
        "phone": "(651) 548 8375",
        "cell": "(663) 854 7534",
        "email": "paulina.parra@example.com",
        "city": "Tlazazalca",
        "state": "Nayarit",
        "country": "Mexico",
    },
]


def test_extract_flat_details_to_file(tmp_path):
    logger = logging.getLogger("root")
    test_data = Path(__file__).parents[0] / "data" / "valid-randomusers.txt"
    flat_data = tmp_path / "flat.txt"

    with open(test_data, "rt") as input_generator:
        extract_flat_details_to_file(logger, input_generator, flat_data)

    expected_data = "\n".join([json.dumps(x) for x in expected_flat_records]) + "\n"

    with open(flat_data, "rt") as f:
        assert f.read() == expected_data


def test_to_avro_file(tmp_path):
    logger = logging.getLogger("root")
    test_data = Path(__file__).parents[0] / "data" / "flat-randomusers.txt"
    avro_data = tmp_path / "randomusers.avro"

    with open(test_data, "rt") as input_generator:
        to_avro_file(logger, input_generator, avro_data)

    with open(avro_data, "rb") as f:
        records = [x for x in DataFileReader(f, DatumReader())]

    assert records == expected_flat_records


def test_to_parquet_file(tmp_path):
    logger = logging.getLogger("root")
    test_data = Path(__file__).parents[0] / "data" / "flat-randomusers.txt"
    parquet_data = tmp_path / "randomusers.parquet"

    with open(test_data, "rt") as input_generator:
        to_parquet_file(logger, input_generator, parquet_data)

    table = pyarrow.parquet.read_table(parquet_data)
    assert table.to_pylist() == expected_flat_records
