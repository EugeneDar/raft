import threading
import json
from utils.constants import *

class Storage:
    def __init__(self):
        self.lock = threading.Lock()
        self.data = {}
    
    def get(self, key):
        with self.lock:
            return self.data.get(key, None)
    
    def set(self, key, value):
        with self.lock:
            self.data[key] = value
    
    def delete(self, key):
        with self.lock:
            if key in self.data:
                del self.data[key]
                return True
            return False

    def handle_log_delivery(self, msg):
        msg = json.loads(msg)
        request_type = msg[REQUEST_TYPE]
        if request_type == GET:
            raise NotImplementedError
        elif request_type == SET:
            self.set(msg[KEY], msg[VALUE])
        elif request_type == DELETE:
            self.delete(msg[KEY])
        else:
            raise ValueError(f"Invalid request type: {request_type}")
