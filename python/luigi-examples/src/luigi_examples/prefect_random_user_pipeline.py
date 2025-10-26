import logging
from time import sleep
from os import makedirs
from prefect import flow, task, tags
from prefect.assets import materialize
from pathlib import Path
from .random_user_functions.random_user_api import (
    download_random_users_to_file,
    validate_random_users_to_file,
)
from .random_user_functions.random_user_to_file import (
    extract_flat_details_to_file,
    validate_data_in_flat_details,
    to_avro_file,
    to_parquet_file,
)


logger = logging.getLogger("prefect")  # not convinced this is right...


workdir = Path(__file__).parents[1] / "prefect-file-outputs"
raw_file = workdir / "raw" / "randomusers.txt"


def file_uri(path: Path) -> str:
    return f"file:/{path.absolute()}"


@materialize(file_uri(raw_file), retries=3, log_prints=True)
def download_random_users(n_record: int):
    print("starting")
    makedirs(raw_file.parent, exist_ok=True)
    download_random_users_to_file(logger, raw_file, n_record=n_record)
    print("done")


@task(asset_deps=[file_uri(workdir / "raw" / "randomusers.txt")], log_prints=True)
def validate_random_users():
    print("starting")
    valid_file = workdir / "validated" / "randomusers.txt"
    invalid_file = workdir / "validation-failes" / "randomusers.txt"
    makedirs(valid_file.parent, exist_ok=True)
    makedirs(invalid_file.parent, exist_ok=True)
    with open(raw_file, "rt") as input_lines:
        validate_random_users_to_file(logger, input_lines, valid_file, invalid_file)
    print("done")


@flow(name="random_users_etl", log_prints=True)
def etl(workdir: Path, n_record: int):
    download_random_users(n_record)
    validate_random_users()


if __name__ == "__main__":
    etl(workdir, 3)
