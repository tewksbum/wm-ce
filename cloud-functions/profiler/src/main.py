"""
Cloud Function to read files uploaded to a bucket, compute stats and save those
to Datastore.
"""
import pandas as pd
import os
from google.cloud import datastore

def get_column_headers(columns):
    """
    Returns only the valid column names (filtering out empty or "Unnamed"
    column headers)
    """
    def empty_col_name(col_name):
        return col_name is None or col_name == ""

    res = []
    for col in columns:
        if empty_col_name(col) or "Unnamed" in col:
            continue
        else:
            res.append(col)
    return res

def get_column_stats(series):
    return {
        "rows" : series[~series.isnull()].shape[0],
        "unique": len(series.unique()),
        "min": series[~series.isnull()].min(),
        "max": series[~series.isnull()].max(),
        "populated": series[~series.isnull()].shape[0] / series.shape[0]
    }

def flatten_stats(col_stats):
    flattened = {}
    for col_name, stats in col_stats.items():
        for key, value in stats.items():
            flat_key = ".".join([col_name, key])
            flattened[flat_key] = value
    return flattened

def save_profiler_stats(file_name, columns, column_headers, flat_col_stats):
    """
    Write the profile stats to Datastore
    """
    client = datastore.Client(namespace="wemade.profiler")

    owner, request_file = os.path.split(file_name)
    request, file_extension = os.path.splitext(request_file)
    
    kind = request
    incomplete_key = client.key(kind)

    profile = datastore.Entity(key=incomplete_key)
    profile["file"] = file_name
    profile["request"] = request
    profile["owner"] = owner
    profile["columns"] = columns
    profile["column_headers"] = column_headers

    for key, value in flat_col_stats.items():
        profile[key] = value

    client.put(profile)

def main(data, context):
    """
    Cloud function entry point.
    """

    file_name = data["name"]
    url = "gs://{}/{}".format(data["bucket"], file_name)

    try:
        df = pd.read_csv(url, sep='\t')
    except UnicodeDecodeError:
        df = pd.read_csv(url, sep='\t', encoding="ISO-8859-1")

    columns = df.shape[1]
    column_headers = get_column_headers(df.columns)

    col_stats = {}

    for i, col in enumerate(df.columns):
        if col in column_headers:
            col_stats[col] = get_column_stats(df[col])
        else:
            col_name = "col_{}".format(i)
            col_stats[col_name] = get_column_stats(df[col])

    flat_col_stats = flatten_stats(col_stats)

    save_profiler_stats(file_name, columns, column_headers, flat_col_stats)
