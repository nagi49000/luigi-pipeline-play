import logging
import json
from pathlib import Path
from luigi_examples.random_user_functions.random_user_to_file import (
    extract_flat_details,
    to_avro,
    to_parquet,
)


def test_extract_flat_details(tmp_path):
    logger = logging.getLogger("root")
    test_data = Path(__file__).parents[0] / "data" / "valid-randomusers.txt"
    flat_data = tmp_path / "flat.txt"

    with open(test_data, "rt") as input_generator:
        extract_flat_details(logger, input_generator, flat_data)

    expected_data = (
        json.dumps(
            {
                "name": "Damanja Hoffer",
                "gender": "female",
                "phone": "(038) 8294795",
                "cell": "(06) 54234226",
                "email": "damanja.hoffer@example.com",
                "city": "Peins",
                "state": "Overijssel",
                "country": "Netherlands",
            }
        )
        + "\n"
        + json.dumps(
            {
                "name": "Paulina Parra",
                "gender": "female",
                "phone": "(651) 548 8375",
                "cell": "(663) 854 7534",
                "email": "paulina.parra@example.com",
                "city": "Tlazazalca",
                "state": "Nayarit",
                "country": "Mexico",
            }
        )
        + "\n"
    )

    with open(flat_data, "rt") as f:
        assert f.read() == expected_data
