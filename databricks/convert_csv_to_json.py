import csv
import json

# pass in the name row that will be used as the key for the json
key_name = "key"

# determine the number of indentation for visual representation of json data
indent_count = 4


def convert_to_json(csv_path, json_path):
    data_dict = {}

    with open(csv_path, encoding="utf-8") as csvf:
        csv_reader = csv.DictReader(csvf)

        # convert each row into dict and append it to data_dict
        for rows in csv_reader:
            key = rows[key_name]
            data_dict[key] = rows

    # open a json writer and use the json.dumps() method to dump the data
    with open(json_path, "w", encoding="utf-8") as jsonf:
        jsonf.write(json.dumps(data_dict, indent=indent_count))


# set the file paths
csv_file_path = (
    r"/Users/benkaan/Desktop/data-sparkling/databricks/data/kafka-events.csv"
)
json_file_path = (
    r"/Users/benkaan/Desktop/data-sparkling/databricks/data/kafka-events.json"
)

convert_to_json(csv_file_path, json_file_path)
