import argparse
from dataclasses import dataclass
from pathlib import Path
from os import path

@dataclass
class Parameters:
    file_output_active: bool
    timestamp_active: bool
    path : str
    file_name : str
    broker_IP : str
    broker_port : int
    topic : str

def parsing_for_parameters(args: argparse.Namespace, parameters:Parameters):
    if args.x:
        parameters.file_output_active = True

    if args.s:
        parameters.timestamp_active = False

    if args.o is not None:
        # check if valid and if path or file
        # separate path and filename
        p = Path(args.o)
        if p.exists():
            if p.is_file():
                head, tail = path.split(p)
                parameters.path = head
                parameters.file_name = tail
            else:
                parameters.path = str(p)
        else:
            head, tail = path.split(p)
            if Path(head).is_dir():
                parameters.path = head
                parameters.file_name = tail

    if args.i is not None:
        if string_is_valid_ip4_address(args.i):
            parameters.broker_IP = args.i
        else:
            print(f"'{args.i}' is no valid IP4-address!")

    if args.p is not None and args.p.isnumeric():
        parameters.broker_port = int(args.p)

    if args.t is not None:
        # no sanity check, handle exception for subscribe-method
        parameters.topic = args.t

def string_is_valid_ip4_address(string:str):
    parts = string.split(".")

    if len(parts) != 4:
        return False

    for octet in parts:
        if octet.isnumeric():
            octet = int(octet)
            if octet <0 or octet>255:
                return False
        else:
            return False

    return True