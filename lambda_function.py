#!/bin/env python
import boto3
import pandas as pd
import os
import sys
import json
import qwl_functions

GTM_ENDPOINT_NAME = os.environ['GTM_ENDPOINT_NAME']
MODEL_TIME_STAMP = os.environ['MODEL_TIME_STAMP']

def lambda_handler(event, context):
    response_json = {
        "output":[]}

    print("Received event: " + json.dumps(event, indent=2))

    
    data_main = json.loads(json.dumps(event))
    response_json['model_timestamp'] =MODEL_TIME_STAMP

    for _payload in data_main['payload']:
        advanced_insights ={}
        data = _payload
        input_data = pd.DataFrame([data])

        initial_price = data['Price__c']
        adj_price = data['NetPrice__c']
        part_oid = data['CatalogNode__c']
        bid_quantity = data['BidQuantity__c']
        lot_revenue = data['Lot__r.Sum_of_Bid_Revenue__c']
        if initial_price == 0:
            input_data['price_drop'] = 0
        elif adj_price == 0 or adj_price is None:
            input_data['price_drop'] = 0
        else:
            input_data['price_drop'] = adj_price / initial_price

        parts_df = pd.read_csv("part_map.csv")

        def get_part_score(part):
            pga = 0
            for index, row in parts_df.iterrows():
                if row['REVVY__CatalogNode__c'] == part:
                    pga = row['part_graded_value']
                    break
                else:
                    continue
            return pga
        def get_revenue_ratio_to_lot (x, y, sum_rev):
            if sum_rev == 0 or sum_rev is None:
                return 0
            else:
                return (x*y)/sum_rev
        
        input_data['part_graded_value'] = get_part_score(part_oid)
        input_data['revenue_ratio_to_lot'] = get_revenue_ratio_to_lot(adj_price, bid_quantity, lot_revenue )
        qwl_input_data_ini = input_data[['price_drop', 'part_graded_value', 'revenue_ratio_to_lot']]
        qwl_input_data = qwl_input_data_ini.to_csv(index=False, header=False)
        #display(qwl_input_data)

        initial_output_from_qwl_model = qwl_functions.initial_qwl_hit(GTM_ENDPOINT_NAME, qwl_input_data)
        print("initial_hit_output")
        print(initial_output_from_qwl_model)
        if float(initial_output_from_qwl_model) >= 0:
            advanced_insights['win_probability'] = float(initial_output_from_qwl_model)
        else:
            advanced_insights['win_probability'] = -1
        
        qwl_input_data_matrix = qwl_input_data_ini
        qwl_input_data_ini.price_drop = 1
        while float(qwl_input_data_ini.price_drop) > 0.0:
            qwl_input_data_matrix = qwl_input_data_matrix.append(qwl_input_data_ini)
            qwl_input_data_ini.price_drop = qwl_input_data_ini.price_drop-0.01
            # qwl_input_data_ini.TotalDiscount__c =qwl_input_data_ini.TotalDiscount__c+0.01
            # if float(qwl_input_data_ini.price_drop) <= 0.5:
            #     break
        qwl_input_data_price_play = qwl_input_data_matrix.to_csv(index=False, header=False)

        price_play_output_from_qwl_model = qwl_functions.price_play_qwl_hit(GTM_ENDPOINT_NAME, qwl_input_data_price_play)
        
        #price_play_output_from_qwl_model =price_play_output_from_qwl_model[0].split('\n')
        price_play_output_from_qwl_model.pop()
        #float_values = [float(x) for x in price_play_output_from_qwl_model]
        print("pp values")
        print(price_play_output_from_qwl_model)
        #print(float_values)
        qwl_input_data_matrix['predict_values'] = price_play_output_from_qwl_model
        print(qwl_input_data_matrix)
        qwl_output = qwl_input_data_matrix[['price_drop', 'predict_values']]
        print(qwl_output)
        qwl_output = qwl_output.astype('float')
        qwl_output = qwl_output.clip(lower=0)
        qwl_output['ema']=qwl_output['predict_values'].ewm(com=37).mean() #this line is no longer used in the for graphical plotting, sending direct pedict values instead
        qwl_output_final = qwl_output[['price_drop', 'predict_values']]
        print(qwl_output)
        if advanced_insights["win_probability"] <=0:
            advanced_insights["price_probability"] ="NA - Base win probability is a grabage value, please check your input block"
        else:
            advanced_insights["price_probability"] =qwl_output_final.to_json( orient ='values')
        response_json["output"].append(advanced_insights)

    return (response_json)
