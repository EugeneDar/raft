import threading
from datetime import datetime


lock = threading.Lock()

def log(text):
    now = datetime.now()
    formatted_time = '(' + now.strftime("%H:%M:%S.%f")[:-3] + ') '
    with lock:
        print(formatted_time + text)
