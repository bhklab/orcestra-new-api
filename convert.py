#Used to convert MongoDB documents into JSON format
from bson import ObjectId
from typing import Any

def convert(data: Any) -> Any:
    if isinstance(data, list):
        return [convert(item) for item in data]
    if isinstance(data, dict):
        return {key: convert(value) for key, value in data.items()}
    if isinstance(data, ObjectId):
        return str(data)
    return data
