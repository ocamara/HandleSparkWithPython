from pyspark import SparkContext
from pyspark.sql import SparkSession
from functions import *

spark = SparkSession\
    .builder\
    .appName("Handle Csv Files")\
    .getOrCreate()


def create_dataframe():
    sc = spark.sparkContext
    file = "resources/data"
    data = sc.textFile(file)
    header = data.first()
    lines = data.filter(lambda line: line != header)
    rows = lines.map(lambda line: handle_row(line))
    columns = get_columns()
    schema = create_schema(columns)
    df = spark.createDataFrame(rows, schema)
    df.show()
    df.printSchema()


def handle_row(line):
    parts = line.split(";")
    id = get_value(parts, 0, True)
    name = get_value(parts, 1, True)
    telephone = delete_string(".", delete_space(get_value(parts, 2, True)))
    salary = to_float(get_value(parts, 3, True))
    date = to_date(get_value(parts, 4, True), "%d-%m-%Y %H:%M:%S")
    json_id, json_num, json_rue, json_ecole_nom, json_formation_num, json_formation_rue, json_niveau = parse_json(parts)
    return id, name, telephone, salary, date, json_id,json_num, json_rue, json_ecole_nom, json_formation_num, json_formation_rue, json_niveau


def parse_json(parts):
    json_values = get_value(parts, 5)
    values = to_json(json_values)
    json_id = get_value(values, "id")
    json_adresse = get_value(values, "adresse")
    json_num = get_value(json_adresse, "num")
    json_rue = get_value(json_adresse, "rue")
    json_formation = get_value(values, "formation")
    json_ecole = get_value(json_formation, "ecole")
    json_ecole_nom = get_value(json_ecole, "nom")
    json_ecole_adresse = get_value(json_ecole, "adresse")
    json_formation_num = get_value(json_ecole_adresse, "num")
    json_formation_rue = get_value(json_ecole_adresse, "rue")
    json_niveau = get_value(json_formation, "niveau")
    return json_id,json_num, json_rue, json_ecole_nom, json_formation_num, json_formation_rue, json_niveau

def get_columns():
    columns = [("id", "string"), ("name", "string"), ("telephone", "string"), ("salary", "double"), ("date", "time")]
    columns_json = [("id", "string"), ("num-adresse", "string"), ("nom-rue", "string"), ("nom-ecole", "string"),
                    ("ecole-adresse-num", "string"), ("ecole-adresse-rue", "string"), ("formation-niveau", "string")]
    columns.extend(columns_json)
    return columns

