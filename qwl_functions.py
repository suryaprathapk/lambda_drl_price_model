import boto3
import pandas as pd

def initial_qc_hit(endpoint_name, payload_body):
    client = boto3.client('sagemaker-runtime')
    response = client.invoke_endpoint(
    EndpointName = endpoint_name,
    Body = payload_body,
    ContentType = 'text/csv',
    Accept = 'text/csv'
    )
    qc_initial_response = response['Body'].read().decode("utf-8")
    return float(qc_initial_response)

def initial_qwl_hit(endpoint_name, payload_body):
    client = boto3.client('sagemaker-runtime')
    response = client.invoke_endpoint(
    EndpointName = endpoint_name,
    Body = payload_body,
    ContentType = 'text/csv',
    Accept = 'text/csv'
    )
    qwl_initial_response = response['Body'].read().decode("utf-8")
    return float(qwl_initial_response)

def price_play_qwl_hit(endpoint_name, payload_body):
    client = boto3.client('sagemaker-runtime')
    response = client.invoke_endpoint(
    EndpointName = endpoint_name,
    Body = payload_body,
    ContentType = 'text/csv',
    Accept = 'text/csv'
    )
    qwl_price_play_reponse = response['Body'].read().decode("utf-8")
    return qwl_price_play_reponse.split('\n')

def price_play_output_parser(dataframe, pp_threshold):
    if dataframe['predict_values'].iloc[0] < pp_threshold:
        for index, row in dataframe.iterrows():
            if row['predict_values'] <pp_threshold:
                continue
            else:
                return(row['price_drop'])
                break
        return -1
    if dataframe['predict_values'].iloc[0] > pp_threshold:
        for index, row in dataframe.iterrows():
            if row['predict_values'] >pp_threshold:
                continue
            else:
                return(row['price_drop'])
                break
        return (row['price_drop'])

def optimized_price_parser(dataframe):
    opt_row = dataframe[dataframe.predict_values == dataframe.predict_values.max()]
    opt_row_final = opt_row[opt_row.price_drop == opt_row.price_drop.max()]
    opt_row_final['price_drop'].iloc[0]
    return opt_row_final['price_drop'].iloc[0].astype(float), opt_row_final['predict_values'].iloc[0].astype(float)