import argparse

from botocore.exceptions import ClientError

from utils.helper_functions import *

def start_crawler():
    args = parse_args()

    try:
        glue_client.start_crawler(Name=args.crawler_name)
    except ClientError as e:
        logging.error(e)
        return False
    return True

def parse_args():
    parser = argparse.ArgumentParser(description="Arguments required for script")
    parser.add_argument('-c', '--crawler_name', required=True, help='Name of Crawler')

    args = parser.parse_args()
    return args

if __name__ == '__main__':
    start_crawler()
