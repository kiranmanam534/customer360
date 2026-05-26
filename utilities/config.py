import sys
import os

base_path = os.path.abspath(os.path.join(os.getcwd(), ".."))
sys.path.append(base_path)

from utilities.constants import *


class Configuration:
    def __init__(self):
        self.storage_account = STORAGE_ACCOUNT
        self.storage_container = STORAGE_CONTAINER
        self.base_path = (
            f"abfss://{self.storage_container}"
            f"@{self.storage_account}.dfs.core.windows.net"
        )

    def get_path(self, schema, folder=None, file=None):
        if folder and file:
            return f"{self.base_path}/{schema}/{folder}/{file}"
        elif folder:
            return f"{self.base_path}/{schema}/{folder}"
        elif file:
            return f"{self.base_path}/{schema}/{file}"
        return f"{self.base_path}/{schema}"


config = Configuration()