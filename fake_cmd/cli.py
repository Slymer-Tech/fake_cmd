import argparse
from fake_cmd.core.client import CLI
from fake_cmd.core.server import Server
from fake_cmd.utils.common import parser_parse


def client_main():
    parser = argparse.ArgumentParser()
    parser.add_argument("address", type=str)
    parser.add_argument("--id_prefix", type=str, default=None, required=False)
    args = parser_parse(parser)
    CLI(args.address, args.id_prefix).run()


def server_main():
    parser = argparse.ArgumentParser()
    parser.add_argument("address", type=str)
    parser.add_argument("--max_cmds", type=int, default=None, required=False)
    args = parser_parse(parser)
    Server(args.address, args.max_cmds).run()
