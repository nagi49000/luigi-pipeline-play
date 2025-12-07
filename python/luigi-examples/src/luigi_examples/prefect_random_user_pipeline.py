from datetime import datetime
from time import sleep
from os import makedirs, getenv
from prefect import flow, task, tags
from prefect.assets import materialize
from prefect.logging import get_run_logger
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


# TODO a bit pants that these paths and files are hard coded
workdir = Path(__file__).parents[1] / "prefect-file-outputs" / datetime.utcnow().strftime("%Y-%m-%dT%H-%M")
raw_file = workdir / "raw" / "randomusers.txt"
valid_file = workdir / "validated" / "randomusers.txt"
invalid_file = workdir / "validation-failed" / "randomusers.txt"
flattened_file = workdir / "flattened" / "randomusers.txt"
valid_flattened_file = workdir / "flattened-validated" / "randomusers.txt"
invalid_flattened_file = workdir / "flattened-validation-failed" / "randomusers.txt"
avro_file = workdir / "avro" / "randomusers.avro"
parquet_file = workdir / "parquet" / "randomusers.parquet"


def file_uri(path: Path) -> str:
    return f"file:/{path.absolute()}"


def get_line_count(filename: Path) -> int:
    n_line = 0
    with open(filename, "rt") as file_obj:
        for _ in file_obj:
            n_line += 1
    return n_line


def materialize_download_random_users(raw_file: Path, n_retries: int, n_record: int):
    @materialize(
        file_uri(raw_file),
        retries=3,
        log_prints=True
    )
    def download_random_users(n_record: int):
        makedirs(raw_file.parent, exist_ok=True)
        download_random_users_to_file(get_run_logger(), raw_file, n_record=n_record)

    return download_random_users(n_record)


@materialize(
    file_uri(valid_file),
    asset_deps=[file_uri(raw_file)],
    log_prints=True
)
def validate_random_users():
    makedirs(valid_file.parent, exist_ok=True)
    makedirs(invalid_file.parent, exist_ok=True)
    with open(raw_file, "rt") as input_lines:
        validate_random_users_to_file(get_run_logger(), input_lines, valid_file, invalid_file)


@materialize(
    file_uri(invalid_file),
    asset_deps=[file_uri(valid_file)],
    log_prints=True
)
def invalid_random_users():
    """ Bit of a dummy task, since validate_random_users really makes 2 assets """
    n_invalid = get_line_count(invalid_file)
    print(f"Found {n_invalid} invalid records in {invalid_file}")


@materialize(
    file_uri(flattened_file),
    asset_deps=[file_uri(valid_file)],
    log_prints=True
)
def extract_flat_details():
    makedirs(flattened_file.parent, exist_ok=True)
    with open(valid_file, "rt") as input_lines:
        extract_flat_details_to_file(get_run_logger(), input_lines, flattened_file)


@materialize(
    file_uri(valid_flattened_file),
    asset_deps=[file_uri(flattened_file)],
    log_prints=True
)
def validate_flat_details():
    makedirs(valid_flattened_file.parent, exist_ok=True)
    makedirs(invalid_flattened_file.parent, exist_ok=True)
    with open(flattened_file, "rt") as input_lines:
        validate_data_in_flat_details(get_run_logger(), input_lines, valid_flattened_file, invalid_flattened_file)


@materialize(
    file_uri(invalid_flattened_file),
    asset_deps=[file_uri(valid_flattened_file)],
    log_prints=True
)
def invalid_flat_details():
    """ Bit of a dummy task, since validate_flat_details really makes 2 assets """
    n_invalid = get_line_count(invalid_flattened_file)
    print(f"Found {n_invalid} invalid flat records in {invalid_file}")


@materialize(
    file_uri(avro_file),
    asset_deps=[file_uri(valid_flattened_file)],
    log_prints=True
)
def to_avro():
    makedirs(avro_file.parent, exist_ok=True)
    with open(valid_flattened_file, "rt") as input_lines:
        to_avro_file(get_run_logger(), input_lines, avro_file)


@materialize(
    file_uri(parquet_file),
    asset_deps=[file_uri(valid_flattened_file)],
    log_prints=True
)
def to_parquet():
    makedirs(parquet_file.parent, exist_ok=True)
    with open(valid_flattened_file, "rt") as input_lines:
        to_parquet_file(get_run_logger(), input_lines, parquet_file)


@flow(name="random_users_etl", log_prints=True)
def random_users_etl(n_record: int = 20):
    """ For some things to work, the flow name has to be the same as the function name :-(
        This can be directly imported by prefect as a deployment
    """
    # download_random_users(n_record)
    materialize_download_random_users(raw_file, 3, n_record)
    validate_random_users()
    invalid_random_users()
    extract_flat_details()
    validate_flat_details()
    invalid_flat_details()
    to_avro()
    to_parquet()


if __name__ == "__main__":
    # can set the server to hook up to with env var, e.g.
    # export PREFECT_API_URL="http://localhost:4200/api"
    # above is the default API url published when running, in a terminal, "prefect server start"
    if getenv("PREFECT_API_URL") is None:  # if no server available, use temp server which will be created...
        random_users_etl(n_record=20)
        sleep(0.5)  # hack to get round waiting on a future at end of pipeline before temp server is shutdown
    else:  # ... otherwise use available server to make a deployment, and treat this terminal as a temp worker pool
        # can run with different parameters, e.g. n_record, by running a deployment
        # https://docs.prefect.io/v3/how-to-guides/deployments/run-deployments
        print(f"using Prefect server at {getenv('PREFECT_API_URL')}")
        print("if possible, when using a Prefect server, also create and use a worker for running flows")
        random_users_etl.serve(name="random-user-deployment", cron="10 * * * *")
