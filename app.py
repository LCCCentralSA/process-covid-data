import warnings
warnings.filterwarnings("ignore")
import pandas as pd
import numpy as np
import awswrangler as wr
from flask import Flask, render_template, request, session, url_for, redirect, jsonify
import os

app = Flask(__name__)
BUCKET_NAME = os.getenv("BUCKET_NAME", "covid19-analysis-test")
DATABASE_NAME = os.getenv("DATABASE_NAME", "covid19-database")

@app.route("/process", methods=["GET", "POST"])
def process():
    if request.method == 'GET':
        return jsonify({"message": "POST request expected"})
    json_data = request.json

    file_name = json_data["file_name"]
    # Change for read data from S3 using aws wrangler
    try:
        print("[INFO] Reading Data from S3")
        path1 = f"s3://{BUCKET_NAME}/to_process/{file_name}"
        df_covid = wr.s3.read_csv([path1])
        # data_path = f"./{file_name}"
        # df_covid = pd.read_csv(data_path, sep=',')
    except Exception as e:
        print(f"[ERROR] {e}")
        raise e
    
    print("[INFO] Building new Dataset")
    # Get only the lastest cases of Covid from each State
    df_recent_cases = df_covid.loc[df_covid['is_last'] == True]
    df_state_dataset_last = df_recent_cases.loc[df_recent_cases["place_type"] == "state"]
    state_cases_death = df_state_dataset_last[['state', 'confirmed', 'deaths']]

    # Creating Dataset from dataframe cases
    df_datetime_cases = df_covid.loc[df_covid['is_last'] == False]
    df_datetime_cases['date'] = pd.to_datetime(df_datetime_cases['date'], format='%Y-%m-%d')
    df_datetime_cases_state = df_datetime_cases.loc[df_datetime_cases["place_type"] == "state"]
    df_datetime_cases_state = df_datetime_cases_state[['date','state', 'confirmed', 'deaths']]


    # Upload to Amazon S3 and create Athena Table with AWS Wrangler
    # Storing data on Data Lake

    if DATABASE_NAME not in wr.catalog.databases().values:
        wr.catalog.create_database(DATABASE_NAME)
    
    print("[INFO] Writing data in S3")
    wr.s3.to_parquet(
        df=state_cases_death,
        path=f"s3://{BUCKET_NAME}/processed/agreggated-table",
        dataset=True,
        database=DATABASE_NAME,
        table="covid-brazil-state",
        mode="overwrite"
    )

    wr.s3.to_parquet(
        df=df_datetime_cases_state,
        path=f"s3://{BUCKET_NAME}/processed/dataframe-table",
        dataset=True,
        database=DATABASE_NAME,
        table="covid-brazil-datetime",
        mode="overwrite"
    )

    return jsonify({"message" : f"File {file_name} processed"})

if __name__ == "__main__":
    app.run(debug=True, host='0.0.0.0')