import sys

def main(args):
    print("twitter_collection.py")
    twitter_collection.main(args)
    args.script = "unmodified"
    print("twitter_storage.py - unmodified")
    twitter_storage.main(args)
    args.script = "modified"
    print("twitter_storage.py - modified")
    twitter_storage.main(args)


if __name__ == "__main__":
    import argparse
    parser = argparse.ArgumentParser()
    parser.add_argument('python_path', help='The connection string')
    parser.add_argument('google_key_path', help='The path of the Google key')
    parser.add_argument('param_connection_string', help='The connection string')
    parser.add_argument('script', help='modified or unmodified')
    args = parser.parse_args()
    sys.path.insert(0, args.python_path)
    import twitter_collection as twitter_collection
    import twitter_storage as twitter_storage
    main(args)