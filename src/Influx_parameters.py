from dataclasses import dataclass
from os import environ
from dotenv import dotenv_values

@dataclass(frozen=True)
class InfluxParameters:
    url: str
    token: str
    org: str
    bucket: str

    @classmethod
    def from_env_file(cls, path: str) -> "InfluxParameters":
        env = dotenv_values(path) | environ
        return cls(
            url=env["INFLUX_URL"],
            token=env["INFLUX_TOKEN"],
            org=env["INFLUX_ORG"],
            bucket=env["INFLUX_BUCKET"],
        )