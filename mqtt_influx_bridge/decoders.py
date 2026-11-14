from .model import RawMessage, SensorSpec, Reading
from .errors import DecodeError
import json

CASTS = {"float": float, "int": int, "str": str, "bool": bool}

def decode_json(msg: RawMessage, spec: SensorSpec) -> list[Reading]:
    try:
        payload:str = msg.payload.decode()
        payload:dict = json.loads(payload)
    except ValueError as e:
        raise DecodeError(msg.topic, msg.payload,  f"cannot parse payload: {e}") from e

    if type(payload) != dict:
        raise DecodeError(msg.topic, msg.payload, f"json.loads doesn't return a dictionary")

    fields_measured = {}
    for key, value in spec.fields.items():
        temp = payload
        contains_key = True
        for level in value.split("."):
            if not isinstance(temp,dict):
                raise DecodeError(msg.topic, msg.payload, f"field {key!r}: {value!r} walks into a non-object")
            if temp.get(level) is None:
                contains_key = False
                break
            temp = temp[level]
        if contains_key:
            if isinstance(temp, (dict, list)):
                raise DecodeError(msg.topic, msg.payload, f"field {key!r}: {value!r} is not a scalar")
            try:
                fields_measured[key] = CASTS[spec.cast](temp)
            except (ValueError, TypeError) as e:
                raise DecodeError(msg.topic, msg.payload, f"field {key!r}: cannot cast {temp!r} to {spec.cast}") from e

    if fields_measured != {}:
        result = Reading(spec.measurement, fields_measured, {}, msg.received_at )
        return [result]
    else:
        return []
