import sys
import json

def main(args):

    with open(args.file1, "r") as f1, open(args.file2, "a") as f2:
        for line in f1.readlines():
            data = json.loads(line)
            data["news_topics"] = str(data["news_topics"])
            f2.write(json.dumps(data, cls=MongoEncoder) + '\n')

if __name__ == "__main__":
    import argparse
    parser = argparse.ArgumentParser()
    parser.add_argument('python_path', help='The connection string')
    parser.add_argument('file1', help='The path of the Google key')
    parser.add_argument('file2', help='The connection string')
    args = parser.parse_args()
    sys.path.insert(0, args.python_path)
    from utils.Storage import Storage, MongoEncoder
    main(args)