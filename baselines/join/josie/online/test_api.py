import os
import argparse
from online_api_gt import api
import time


def test_api(args):
     
    qpath = args.qpath
    save_root = args.save_root
    result_root = args.result_root
    k = args.k

    api(qpath, save_root, result_root, k)

if __name__ == "__main__":
    start_time = time.time()
    parser = argparse.ArgumentParser(description="")
#     parser.add_argument('--lake', type=str, required=True, choices=["webtable", "opendata"])
    parser.add_argument('--qpath', type=str, required=True)
    parser.add_argument('--save_root', type=str, required=True)
    parser.add_argument('--result_root', type=str, required=True)
    parser.add_argument('--k', type=int, required=True)
    args = parser.parse_args()
    test_api(args)
    end_time = time.time()
    exc_time = end_time - start_time
    print("exc_time",exc_time)
