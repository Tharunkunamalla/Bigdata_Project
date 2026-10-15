import socket

def check(p):
    s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    s.settimeout(2.0)
    r = s.connect_ex(("127.0.0.1", p))
    s.close()
    return "OPEN" if r == 0 else "CLOSED"

p9083 = check(9083)
p10000 = check(10000)
print(f"Port 9083 (Hive Metastore Thrift): {p9083}")
print(f"Port 10000 (HiveServer2): {p10000}")
