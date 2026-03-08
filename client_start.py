"""
An example to start the client.
"""

import argparse
from fake_cmd.core.client import CLI
from fake_cmd.utils.common import parser_parse


def get_args():
    parser = argparse.ArgumentParser()
    parser.add_argument("--address", type=str, required=True)
    parser.add_argument("--id_prefix", type=str, default=None, required=False)
    return parser_parse(parser)


if __name__ == "__main__":
    args = get_args()
    CLI(args.address, args.id_prefix).run()
