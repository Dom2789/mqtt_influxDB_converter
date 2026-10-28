from dataclasses import dataclass
import os
from dotenv import load_dotenv

@dataclass
class Influx_parameters:
    url : str = ""
    token : str = ""
    org : str = ""
    bucket : str = ""

    @classmethod
    def read_env_file(cls, path:str):
        load_dotenv(path)
        cls.url = os.environ["INFLUX_URL"]
        cls.token = os.environ["INFLUX_TOKEN"]
        cls.org = os.environ["INFLUX_ORG"]
        cls.bucket = os.environ["INFLUX_BUCKET"]
        return cls