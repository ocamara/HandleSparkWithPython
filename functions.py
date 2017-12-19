import json
from datetime import datetime
from pyspark.sql.types import *


def delete_string(string_delete, string):
    return string.replace(string_delete,"")


def delete_space(string):
    return delete_string(" ",string)


def to_int(string):
    return int(delete_space(string))


def to_float(string):
    return float(delete_space(string))


def to_json(string):
    try:
        return json.loads(string)
    except:
        return {}


def get_value(tab_dict, index, ifBool = False):
    try:
        val = tab_dict[index]
        if ifBool == True:
            return str(tab_dict[index]).replace("\"","")
        return tab_dict[index]
    except:
        return ""


def to_date(string, date_format):
    try:
        return datetime.strptime(string, date_format)
    except ValueError:
        return ""


def create_field(field_name, field_type):
    if field_type == "float":
        return StructField(field_name, FloatType(), True)
    elif field_type == "double":
        return StructField(field_name, DoubleType(), True)
    elif field_type == "int":
        return StructField(field_name, IntegerType(), True)
    elif field_type == "date":
        return StructField(field_name, DateType(), True)
    elif field_type == "time":
        return StructField(field_name, TimestampType(), True)
    else:
        return StructField(field_name, StringType(), True)


def create_schema(columns):
    types = []
    for column in columns:
        field = create_field(column[0], column[1])
        types.append(field)
    return StructType(types)


def get_dict(ref, input, list=[]):
    for key in input.keys():
        data = input [key]
        if type(data) is types.DictType:
            get_dict(ref+ key,data,list)
        else:
            list.extend([(ref + key,data)])
