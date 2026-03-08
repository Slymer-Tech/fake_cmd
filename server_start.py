"""
An example to start the server.
"""

import argparse
from fake_cmd.core.server import Server
from fake_cmd.utils.common import parser_parse


def get_args():
    parser = argparse.ArgumentParser()
    parser.add_argument("--address", type=str, required=True)
    parser.add_argument("--max_cmds", type=int, default=None, required=False)
    return parser_parse(parser)


if __name__ == "__main__":
    args = get_args()
    Server(args.address, args.max_cmds).run()
