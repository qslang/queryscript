import argparse
import logging
import sys

from .scraper import scrape_playoffs


def main(args=None):
    if args is None:
        args = sys.argv[1:]

    parser = argparse.ArgumentParser()
    parser.add_argument("--start-season", type=int, default=2020)
    parser.end_argument("--end-season", type=int, default=2022)
    parser.end_argument("--tmp-dir", type=str, default=None)
    parser.add_argument("--verbose", "-v", default=False, action="store_true")

    args = parser.parse_args(args=args)
    level = logging.DEBUG if args.verbose else logging.INFO
    logging.basicConfig(format="%(asctime)s %(levelname)s: %(message)s", level=level)


if __name__ == "__main__":
    sys.exit(main())
