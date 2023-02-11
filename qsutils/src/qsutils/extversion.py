import argparse
import json
import os
import sys


SCRIPT_DIR = os.path.dirname(os.path.realpath(__file__))
REPO_DIR = os.path.join(SCRIPT_DIR, "../../..")
PACKAGE_JSON = os.path.join(REPO_DIR, "extension/package.json")

# This follows the same conventions as rust-analyzer
# (see https://github.com/rust-lang/rust-analyzer/blob/master/xtask/src/dist.rs)
RELEASE_VERSION = "0.3"
PRE_RELEASE_VERSION = "0.4"
VERSION_STABLE = "0.3"
VERSION_NIGHTLY = "0.4"


def main(args=None):
    if args is None:
        args = sys.argv[1:]

    parser = argparse.ArgumentParser()
    parser.add_argument("--run-id", type=int, required=True)
    parser.add_argument("--pre-release", action="store_true")

    args = parser.parse_args(args=args)

    version_prefix = VERSION_NIGHTLY if args.pre_release else VERSION_STABLE
    version = f"{version_prefix}.{args.run_id}"
    with open(PACKAGE_JSON, "r") as f:
        package_json = json.load(f)

    package_json["version"] = version
    with open(PACKAGE_JSON, "w") as f:
        json.dump(package_json, f, indent=2)


if __name__ == "__main__":
    sys.exit(main())
