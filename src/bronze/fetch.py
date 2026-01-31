import argparse
import subprocess
import sys


def run(cmd: list[str]) -> None:
    """
    Runs the bash commands as arguments.
    
    :param cmd: Bash commands 
    :type cmd: list[str]
    """
    print(f"[fetch] run: {' '.join(cmd)}")
    res = subprocess.run(cmd)
    if res.returncode != 0:
        raise SystemExit(res.returncode)


def main() -> None:
    """
    Set parameters --from --to, and calls download and extract bash scripts.
    """
    p = argparse.ArgumentParser()
    p.add_argument("--from", dest="from_month", required=True, help="YYYY-MM")
    p.add_argument("--to", dest="to_month", required=True, help="YYYY-MM")
    args = p.parse_args()

    print(f"[fetch] start from={args.from_month} to={args.to_month}")

    # 1) download each month from the interval
    run(["bash", "src/bronze/comext_download.sh", args.from_month, args.to_month])

    # 2) extract every month once download completes
    run(["bash", "src/bronze/comext_extract.sh"])

    print("[fetch] end")


if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        print("\n[fetch] interrupted", file=sys.stderr)
        raise SystemExit(130)