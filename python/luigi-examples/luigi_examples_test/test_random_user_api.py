import logging
from pathlib import Path
from luigi_examples.random_user_functions.random_user_api import (
    download_random_users,
    validate_random_users,
)


def test_download_random_users(tmp_path, requests_mock, caplog):
    logger = logging.getLogger("root")
    outfile = tmp_path / "randomusers.txt"

    requests_mock.get("https://randomuser.me/api/", text='foo')
    download_random_users(logger, outfile, n_record=4)
    with open(outfile, "rt") as f:
        assert f.read() == "foo\nfoo\nfoo\nfoo\n"

    requests_mock.get("https://randomuser.me/api/", text='bar')
    download_random_users(logger, outfile, n_record=2)
    with open(outfile, "rt") as f:
        assert f.read() == "bar\nbar\n"

    requests_mock.get("https://randomuser.me/api/", status_code=404, reason="deliberate for test")
    download_random_users(logger, outfile)
    with open(outfile, "rt") as f:
        assert f.read() == ""

    error_msg = "ERROR    root:random_user_api.py:20 GET to https://randomuser.me/api/ failed with 404: deliberate for test\n"
    # hit API 3 times, so get 3 error messages
    assert caplog.text == error_msg + error_msg + error_msg


def test_validate_random_users(tmp_path, caplog):
    logger = logging.getLogger("root")
    test_data = Path(__file__).parents[0] / "data" / "randomusers.txt"
    valid_data = tmp_path / "valid.txt"
    invalid_data = tmp_path / "invalid.txt"

    with open(test_data, "rt") as input_generator:
        validate_random_users(logger, input_generator, valid_data, invalid_data)

    # 2 record fails; 1 incomplete JSON and one not a JSON
    with open(invalid_data, "rt") as f:
        assert f.read() == '{"results":[]}\niamnotajson\n'

    assert "ERROR    root:random_user_api.py:187 'info' is a required property" in caplog.text
    assert "ERROR    root:random_user_api.py:187 Expecting value: line 1 column 1 (char 0)" in caplog.text

    with open(valid_data, "rt") as f:
        valid_rows = f.read().strip("\n").split("\n")
    assert len(valid_rows) == 2
