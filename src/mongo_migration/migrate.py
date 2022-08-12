import json
from json.decoder import JSONDecodeError
from pathlib import Path
from typing import Any, Dict

from mongo_migration.errors import NotValidJsonError


def deserialise(filepath: Path) -> Dict[Any, Any]:
    with open(filepath, "r") as stream:
        try:
            data = json.load(stream)
        except JSONDecodeError:
            raise NotValidJsonError("File provided is not a valid json")

    return data
