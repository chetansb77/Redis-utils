import redis
from typing import Any
import ast

from scripts.helpers.constants import REDIS_HOST, REDIS_PASSWORD


class GetRedisClient:
    def __init__(self):
        self.r = redis.Redis(host=REDIS_HOST, password=REDIS_PASSWORD)

    def get_redis_client_object(self):
        return self.r

    def get_existing_keys(self):
        return self.r.keys()

    def delete_existing_key(self, key: str):
        self.r.delete(key)

    def get_existing_value(self, key: str):
        value = self.r.get(key).decode("utf-8")
        try:
            return ast.literal_eval(value)
        except:
            return value

    def set_key_value_pair(self, key: str, value: Any):
        self.r.mset({key: str(value)})

    def delete_all_existing_keys(self):
        self.r.delete(*self.get_existing_keys())

    def delete_existing_keys_containing_string(self, s: str):
        keys_to_delete = [
            i for i in self.get_existing_keys() if s in i.decode("utf-8")
        ]
        self.r.delete(*keys_to_delete)
