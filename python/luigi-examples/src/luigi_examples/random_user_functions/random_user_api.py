import requests
import json
import os
from pathlib import Path
from jsonschema import validate, ValidationError
from random import choices
from datetime import datetime, UTC
from string import ascii_uppercase


def download_random_users(logger, output_path, n_record=3):
    with open(output_path, "wt") as f:
        for _ in range(n_record):
            response = requests.get("https://randomuser.me/api/")
            if response.ok:
                f.write(response.text)
                f.write("\n")
            else:
                logger.error(
                    f"GET to https://randomuser.me/api/ failed with {response.status_code}: {response.reason}"
                )


def validate_random_users(logger, input_generator, output_path, failed_validation_path):
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

    with open(output_path, "wt") as output_lines:
        for line in input_generator:
            try:
                validate(instance=json.loads(line), schema=schema)
                output_lines.write(line)
            except ValidationError as e:
                logger.error(e)
                random_stamped_str = f"{''.join(choices(ascii_uppercase, k=10))}-{datetime.now(UTC).isoformat()}"
                os.makedirs(failed_validation_path, exist_ok=True)
                filename = (failed_validation_path / random_stamped_str)
                with open(filename, "wt") as f:
                    f.write(line)
