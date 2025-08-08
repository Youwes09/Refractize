import socket
import json
import time

UDP_PORT = 7001

def main():
    sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)

    # Replace this with the IP of the laptop running the receiver
    target_ip = input("Enter target IP to send discovery to: ")

    message = {
        "type": "peer_discovery",
        "peer_info": {
            "peer_id": "test-peer",
            "signaling_port": 8001,
            "discovery_port": UDP_PORT,
            "is_signaling_server": False,
            "priority": 100
        }
    }
    data = json.dumps(message).encode()

    while True:
        sock.sendto(data, (target_ip, UDP_PORT))
        print(f"Sent discovery message to {target_ip}:{UDP_PORT}")
        time.sleep(5)  # send every 5 seconds

if __name__ == "__main__":
    main()
