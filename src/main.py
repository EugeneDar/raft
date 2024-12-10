import threading
import argparse

from utils.cluster_info import CLUSTER_NODES
from deploy.deploy import *


def parse_arguments():
    parser = argparse.ArgumentParser()
    parser.add_argument(
        '--all',
        action='store_true'
    )
    parser.add_argument(
        '--node_id',
        type=str
    )
    return parser.parse_args()


def main():
    args = parse_arguments()
    if args.all and args.node_id is not None:
        print("You can't use --all and --node_id together")
        return
    
    if args.node_id is not None:
        _, _ = create_background_node(args.node_id)
    elif args.all:
        print("Starting Raft cluster...")
        for node_id in CLUSTER_NODES:
            _, _ = create_background_node(node_id)
    else:
        print("You need to specify --all or --node_id")
        return

    sleep_forever()


if __name__ == "__main__":
    main()
