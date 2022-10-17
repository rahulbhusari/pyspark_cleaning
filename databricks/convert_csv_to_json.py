import csv
import json

# OPTIONAL determine the number of indentation for visual representation of json data
indent_count = 4


def convert_to_json(csv_path, json_path):
    json_list = []

    with open(csv_path, encoding="utf-8") as csvf:
        csv_reader = csv.DictReader(csvf)

        # append each row in the dict from csv_reader to data_dict
        for row in csv_reader:
            json_list.append(row)

    # open a json writer and use the json.dumps() method to dump the data
    with open(json_path, "w", encoding="utf-8") as jsonf:
        json_string = json.dumps(json_list, indent=indent_count)
        jsonf.write(json_string)


# set the file paths
csv_file_path = (
    r"/Users/benkaan/Desktop/data-sparkling/databricks/data/kafka-events.csv"
)
json_file_path = (
    r"/Users/benkaan/Desktop/data-sparkling/databricks/data/kafka-events.json"
)

convert_to_json(csv_file_path, json_file_path)
