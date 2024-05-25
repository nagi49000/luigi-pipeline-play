import logging
from pathlib import Path
from luigi_examples.random_user_functions.random_user_api import (
    download_random_users_to_file,
    validate_random_users_to_file,
)


def test_download_random_users_to_file(tmp_path, requests_mock, caplog):
    logger = logging.getLogger("root")
    outfile = tmp_path / "randomusers.txt"

    requests_mock.get("https://randomuser.me/api/", text="foo")
    download_random_users_to_file(logger, outfile, n_record=4)
    with open(outfile, "rt") as f:
        assert f.read() == "foo\nfoo\nfoo\nfoo\n"

    requests_mock.get("https://randomuser.me/api/", text="bar")
    download_random_users_to_file(logger, outfile, n_record=2)
    with open(outfile, "rt") as f:
        assert f.read() == "bar\nbar\n"

    requests_mock.get(
        "https://randomuser.me/api/", status_code=404, reason="deliberate for test"
    )
    download_random_users_to_file(logger, outfile)
    with open(outfile, "rt") as f:
        assert f.read() == ""

    error_msg = "GET to https://randomuser.me/api/ failed with 404: deliberate for test"
    assert error_msg in caplog.text


def test_validate_random_users_to_file(tmp_path, caplog):
    logger = logging.getLogger("root")
    test_data = Path(__file__).parents[0] / "data" / "randomusers.txt"
    valid_data = tmp_path / "valid.txt"
    invalid_data = tmp_path / "invalid.txt"

    with open(test_data, "rt") as input_generator:
        validate_random_users_to_file(logger, input_generator, valid_data, invalid_data)

    # 2 record fails; 1 incomplete JSON and one not a JSON
    with open(invalid_data, "rt") as f:
        assert f.read() == '{"results":[]}\niamnotajson\n'

    assert "'info' is a required property" in caplog.text
    assert "Expecting value: line 1 column 1 (char 0)" in caplog.text

    with open(valid_data, "rt") as f:
        valid_rows = f.read().strip("\n").split("\n")
    assert len(valid_rows) == 2
