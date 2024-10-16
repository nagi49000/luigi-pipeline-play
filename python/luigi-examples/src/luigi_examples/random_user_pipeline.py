import luigi
import logging
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


logger = logging.getLogger("luigi")


class DownloadRandomUsers(luigi.Task):

    workdir = luigi.PathParameter(default=".")
    n_record = luigi.IntParameter(default=3)  # , description="number of times to hit the API")

    def output(self):
        return luigi.LocalTarget(Path(self.workdir) / "raw" / "randomusers.txt")

    def run(self):
        with self.output().temporary_path() as temp_output_path:
            download_random_users_to_file(logger, temp_output_path, n_record=self.n_record)


class ValidateRandomUsers(luigi.Task):

    workdir = luigi.PathParameter(default=".")

    def requires(self):
        return DownloadRandomUsers(workdir=self.workdir)

    def output(self):
        return {
            "valid": luigi.LocalTarget(
                Path(self.workdir) / "validated" / "randomusers.txt"
            ),
            "invalid": luigi.LocalTarget(
                Path(self.workdir) / "validation-failed" / "randomusers.txt"
            ),
        }

    def run(self):
        with self.input().open("r") as input_lines:
            with self.output()["valid"].temporary_path() as valid_path:
                with self.output()["invalid"].temporary_path() as invalid_path:
                    validate_random_users_to_file(logger, input_lines, valid_path, invalid_path)


class ExtractFlatDetails(luigi.Task):

    workdir = luigi.PathParameter(default=".")

    def requires(self):
        return ValidateRandomUsers(workdir=self.workdir)

    def output(self):
        return luigi.LocalTarget(Path(self.workdir) / "flattened" / "randomusers.txt")

    def run(self):
        with self.input()["valid"].open("r") as input_lines:
            with self.output().temporary_path() as temp_output_path:
                extract_flat_details_to_file(logger, input_lines, temp_output_path)


class ValidateFlatDetails(luigi.Task):

    workdir = luigi.PathParameter(default=".")

    def requires(self):
        return ExtractFlatDetails(workdir=self.workdir)

    def output(self):
        return {
            "valid": luigi.LocalTarget(
                Path(self.workdir) / "flattened-validated" / "randomusers.txt"
            ),
            "invalid": luigi.LocalTarget(
                Path(self.workdir) / "flattened-validation-failed" / "randomusers.txt"
            ),
        }

    def run(self):
        with self.input().open("r") as input_lines:
            with self.output()["valid"].temporary_path() as valid_path:
                with self.output()["invalid"].temporary_path() as invalid_path:
                    validate_data_in_flat_details(logger, input_lines, valid_path, invalid_path)


class ToAvro(luigi.Task):

    workdir = luigi.PathParameter(default=".")

    def requires(self):
        return ValidateFlatDetails(workdir=self.workdir)

    def output(self):
        return luigi.LocalTarget(
            Path(self.workdir) / "avro" / "randomusers.avro", format=luigi.format.Nop
        )

    def run(self):
        with self.input()["valid"].open("r") as input_lines:
            with self.output().temporary_path() as temp_output_path:
                to_avro_file(logger, input_lines, temp_output_path)


class ToParquet(luigi.Task):

    workdir = luigi.PathParameter(default=".")

    def requires(self):
        return ValidateFlatDetails(workdir=self.workdir)

    def output(self):
        return luigi.LocalTarget(
            Path(self.workdir) / "parquet" / "randomusers.parquet",
            format=luigi.format.Nop,
        )

    def run(self):
        with self.input()["valid"].open("r") as input_lines:
            with self.output().temporary_path() as temp_output_path:
                to_parquet_file(logger, input_lines, temp_output_path)


class AllSinks(luigi.WrapperTask):

    workdir = luigi.PathParameter(default=".")

    def requires(self):
        return [ToParquet(workdir=self.workdir), ToAvro(workdir=self.workdir)]
