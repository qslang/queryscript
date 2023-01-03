import argparse
import logging
import os
import sys
import tempfile

from .scraper import SCRAPERS, scrape


def main(args=None):
    if args is None:
        args = sys.argv[1:]

    parser = argparse.ArgumentParser()
    parser.add_argument("--start-season", type=int, default=1980)
    parser.add_argument("--end-season", type=int, default=2022)
    parser.add_argument("--tmp-dir", type=str, default=tempfile.gettempdir())
    parser.add_argument("--verbose", "-v", default=False, action="store_true")
    parser.add_argument("--datasets", choices=list(SCRAPERS.keys()), default=list(SCRAPERS.keys()), nargs="+")
    parser.add_argument("out", type=str)

    args = parser.parse_args(args=args)
    level = logging.DEBUG if args.verbose else logging.INFO
    logging.basicConfig(format="%(asctime)s %(levelname)s: %(message)s", level=level)

    os.makedirs(args.out, exist_ok=True)
    os.makedirs(args.tmp_dir, exist_ok=True)

    scrape(args.start_season, args.end_season, set(args.datasets), args.tmp_dir, args.out)


if __name__ == "__main__":
    sys.exit(main())
