import tomllib
from tomli_w import dumps
from collections.abc import Mapping

class ConfigToml(Mapping):
    # inheriting from Mapping gets in, .get(), .keys(), .items(), .values(), and == for free
    def __init__(self, path_config_file:str):
        self._data = {}
        with open(path_config_file, "rb") as f:  # tomllib requires binary mode
            self._data = tomllib.load(f)

    def __str__(self):
        return dumps(self._data)

    def __getitem__(self, key):
        return self._data[key]

    def __iter__(self):
        return iter(self._data)

    def __len__(self):
        return len(self._data)