#
# EarthCODE metadata DOI provvisioning
#
# Project: EarthCODE
#
# Center for Sensing Solutions (Eurac research)
#
# file: process_json.py
#

import json
from datetime import datetime
import os

from types import SimpleNamespace
from typing import Union, Any


def _to_namespace(obj: Any) -> Any:
    if isinstance(obj, dict):
        return SimpleNamespace(**{k: _to_namespace(v) for k, v in obj.items()})
    if isinstance(obj, list):
        return [_to_namespace(item) for item in obj]
    return obj

def process_json(json_file_path):
        # Verifica se il file esiste
    if not os.path.exists(json_file_path):
        print(f"Error: file not found -> '{json_file_path}' ")
        return

    try:
        with open(json_file_path, 'r', encoding='utf-8') as file:
            data = json.load(file)

        return _to_namespace(data)

    except json.JSONDecodeError:
        print("Error: not a valid metadata json file")
    except Exception as e:
        print(f"Error during execution {e}")
