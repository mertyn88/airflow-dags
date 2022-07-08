import socket


def get_active_profile():
    host = socket.gethostbyname(socket.gethostname())
    if host.endswith('20'):
        return 'prod'
    return 'qa'


def get_read_file(path):
    with open(path, 'r') as f:
        return f.read()
