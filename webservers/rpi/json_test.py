import requests
import json
import argparse
import sys
import socket

def send_json_packet(server_url,server_port, command, args):
    # Define the JSON payload
    payload = {
        "command": command,
        "args": args
    }

    # Set the Content-Type header to indicate JSON data
    headers = {"Content-Type": "application/json"}

    try:
        data = json.dumps(payload)
        # Send the POST request with JSON payload
        # response = requests.post(server_url, json=payload, headers=headers)
        response = socket.create_connection((server_url, server_port)).sendall(data.encode('utf-8'))

    except Exception as e:
        print(f"An error occurred: {e}")

if __name__ == "__main__":
    # Create an argument parser
    parser = argparse.ArgumentParser(description="Send a JSON packet to a server.")

    # Define command line arguments
    parser.add_argument("server_address", help="The address of the server (including protocol and NOT the port)")
    parser.add_argument("server_port", help="The port to use")
    parser.add_argument("command", help="The command value for the JSON packet")
    parser.add_argument("args", nargs='+', help="The args value for the JSON packet")

    # Parse the command line arguments
    args = parser.parse_args(sys.argv[1:])

    # Call the function with command line arguments
    send_json_packet(args.server_address, args.server_port, args.command, args.args)