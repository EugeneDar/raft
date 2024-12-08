import threading

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
        # TODO
        raise NotImplementedError
