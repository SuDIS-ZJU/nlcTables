import os
import argparse
from offline_api import api
import time



def test_api(args):
     
    cpath = args.cpath
    save_root = args.save_root

    api(cpath, save_root)

if __name__ == "__main__":
    start_time = time.time()
    parser = argparse.ArgumentParser(description="")
#     parser.add_argument('--lake', type=str, required=True, choices=["webtable", "opendata"])
    parser.add_argument('--cpath', type=str, required=True)
    parser.add_argument('--save_root', type=str, required=True)
    args = parser.parse_args()
    test_api(args)
    end_time = time.time()
    exc_time = end_time - start_time  
    print("exc_time",exc_time)

