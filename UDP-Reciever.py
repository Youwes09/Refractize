import socket

UDP_PORT = 7001
BUFFER_SIZE = 1024

def main():
    sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    sock.bind(('', UDP_PORT))  # Listen on all interfaces on UDP_PORT
    print(f"Listening for discovery messages on UDP port {UDP_PORT}...")

    while True:
        data, addr = sock.recvfrom(BUFFER_SIZE)
        print(f"Received from {addr}: {data.decode()}")

if __name__ == "__main__":
    main()
