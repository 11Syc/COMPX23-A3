import socket
import sys

def send_request(sock, request):
    sock.sendall(request.encode('utf-8'))
    response = sock.recv(1024).decode('utf-8')
    return response

def main():
    if len(sys.argv) != 4:
        print("Usage: python Client.py < Server hostname > < port number > < Request file path >")
        sys.exit(1)
    server_host = sys.argv[1]
    server_port = int(sys.argv[2])
    request_file_path = sys.argv[3]
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:
        sock.connect((server_host, server_port))

        with open(request_file_path, 'r') as file:
            for line in file:
                line = line.strip()
                if not line:
                    continue
                parts = line.split()
                if len(parts) < 2:
                    print(f"Invalid request: {line}")
                    continue
                cmd = parts[0].upper()
                key = parts[1]
                value = ' '.join(parts[2:]) if cmd == 'P' else ''
                #Build the request message
                msg_size = len(key) + len(value) + 4  # 3位大小 + 1位命令 + key + value
                if msg_size > 999:
                    print(f"Request too large: {line}")
                    continue
                #Send requests and receive responses
                request = f"{msg_size:03}{cmd} {key} {value}".strip()
                response = send_request(sock, request)
                print(f"{line}: {response}")

if __name__ == "__main__":
    main()