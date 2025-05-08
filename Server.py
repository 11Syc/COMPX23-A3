import socket
import threading
from collections import defaultdict

tuple_space = {}

#Declare global variables
stats = {
    'total_clients': 0,
    'total_operations': 0,
    'total_reads': 0,
    'total_gets': 0,
    'total_puts': 0,
    'total_errors': 0
}

def handle_client(client_socket, client_address):
    global stats
    stats['total_clients'] += 1
    print(f"The client {client_address} has been connected")

    try:
        while True:
            #Receive data
            data = client_socket.recv(1024).decode('utf-8')
            if not data:
                break
            #Parsing request
            msg_size = int(data[:3])
            cmd = data[3]
            #Extract key and value(If it's put)
            key = data[4:4+msg_size-4].split()[0] 
            value = data[4+len(key)+1:] if cmd == 'P' else '' 
            #Update the information
            stats['total_operations'] += 1
            if cmd == 'R':
                stats['total_reads'] += 1
            elif cmd == 'G':
                stats['total_gets'] += 1
            elif cmd == 'P':
                stats['total_puts'] += 1

            #Handle the request
            if cmd == 'R':
                if key in tuple_space:
                    response = f"{len(f'OK ({key}, {tuple_space[key]}) read'):03}OK ({key}, {tuple_space[key]}) read"
                else:
                    response = f"{len('ERR key does not exist'):03}ERR key does not exist"
                    stats['total_errors'] += 1
            elif cmd == 'G':
                if key in tuple_space:
                    value = tuple_space.pop(key)
                    response = f"{len(f'OK ({key}, {value}) removed'):03}OK ({key}, {value}) removed"
                else:
                    response = f"{len('ERR key does not exist'):03}ERR key does not exist"
                    stats['total_errors'] += 1
            elif cmd == 'P':
                if key not in tuple_space:
                    tuple_space[key] = value
                    response = f"{len(f'OK ({key}, {value}) added'):03}OK ({key}, {value}) added"
                else:
                    response = f"{len('ERR key already exists'):03}ERR key already exists"
                    stats['total_errors'] += 1
            client_socket.sendall(response.encode('utf-8'))

    finally:
        print(f"The client {client_address} has been disconnected")
        client_socket.close()

def print_stats():
    while True:
        #Calculate the average size
        avg_tuple_size = sum(len(k) + len(v) for k, v in tuple_space.items()) / len(tuple_space) if tuple_space else 0
        avg_key_size = sum(len(k) for k in tuple_space.keys()) / len(tuple_space) if tuple_space else 0
        avg_value_size = sum(len(v) for v in tuple_space.values()) / len(tuple_space) if tuple_space else 0

        print(f"Tuple space size: {len(tuple_space)}")
        print(f"Average tuple size: {avg_tuple_size:.2f}")
        print(f"Average key size: {avg_key_size:.2f}")
        print(f"Average value size: {avg_value_size:.2f}")
        print(f"Total number of clients: {stats['total_clients']}")
        print(f"Total operands: {stats['total_operations']}")
        print(f"READ: {stats['total_reads']}")
        print(f"GET: {stats['total_gets']}") 
        print(f"PUT: {stats['total_puts']}")
        print(f"Error: {stats['total_errors']}")
        #Print once every 10 seconds
        threading.Event().wait(10)

def main():
    server_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    server_socket.bind(('localhost', 51234))
    server_socket.listen()
    print("The server has been started and is waiting for the connection...")
    #Start the statistical information printing thread
    threading.Thread(target=print_stats, daemon=True).start()

    try:
        while True:
            client_socket, client_address = server_socket.accept()
            client_handler = threading.Thread(target=handle_client, args=(client_socket, client_address))
            client_handler.start()
    finally:
        server_socket.close()

if __name__ == "__main__":
    main()