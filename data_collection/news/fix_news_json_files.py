from pathlib import Path
import json
import sys

def main(argv):
    path = Path(".")

    for json_file in path.iterdir():
        print(json_file)
        new_name = "fixed/" + str(json_file)

        with open(str(json_file), 'r') as f1, open(new_name, "w") as f2:
            for line in f1:
                item = json.loads(line)

                if item["news_topics"] is None:
                    item["news_topics"] = []
                elif isinstance(item["news_topics"],str):
                    item["news_topics"] = [i.strip() for i in item["news_topics"].split(";")]
                elif isinstance(item["news_topics"],list):
                    item["news_topics"] = [i.strip() for i in item["news_topics"]]

                if item["news_companies"] is None:
                    item["news_companies"] = []
                elif isinstance(item["news_companies"],str):
                    item["news_companies"] = [i.strip() for i in item["news_companies"].split(";")]
                elif isinstance(item["news_companies"],list):
                    item["news_companies"] = [i.strip() for i in item["news_companies"]]

                if item["news_country"] is None:
                    item["news_country"] = []
                elif isinstance(item["news_country"],str):
                    item["news_country"] = [i.strip() for i in item["news_country"].split(";")]
                elif isinstance(item["news_country"], list):
                    item["news_country"] = [i.strip() for i in item["news_country"]]

                if item["news_region"] is None:
                    item["news_region"] = []
                elif isinstance(item["news_region"],str):
                    item["news_region"] = [i.strip() for i in item["news_region"].split(";")]
                elif isinstance(item["news_region"], list):
                    item["news_region"] = [i.strip() for i in item["news_region"]]

                f2.write(json.dumps(item) + '\n')

if __name__ == "__main__":
    main(sys.argv[1:])