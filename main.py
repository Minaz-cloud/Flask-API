try:
    from flask import Flask, request, jsonify
    from flask_restful import Resource, Api
    from flask_restful import reqparse
    from flask_limiter.util import get_remote_address
    from flask_limiter import Limiter
    import pandas as pd
    import numpy as np
    import json
    from datetime import datetime
    import sqlalchemy
    import mysql.connector
    from mysql.connector import Error

except Exception as ex:
    print("Some modules are missing{}".format(ex))

app = Flask(__name__)
api = Api(app)
parser = reqparse.RequestParser()


class CalculateChargesGSTAPI(Resource):
    def __init__(self):
        parser.add_argument('startdate', type=str, required=True, help="Please enter startdate")
        parser.add_argument('enddate', type=str, required=True, help="Please enter enddate")
        parser.add_argument('merchant_id', type=str, required=True, help="Please enter merchant id")
        parser.add_argument('percentage', type=str, required=True, help="Please enter percentage")
        parser.add_argument('flat_amount', type=str, required=True, help="Please enter flat amount")
        parser.add_argument('gst', type=str, required=True, help="Please enter gst")

        self.__startdate = parser.parse_args().get('startdate', None)
        self.__enddate = parser.parse_args().get('enddate', None)
        self.__merchant_id = parser.parse_args().get('merchant_id', None)
        self.__percentage = parser.parse_args().get('percentage', None)
        self.__flat_amount = parser.parse_args().get('flat_amount', None)
        self.__gst = parser.parse_args().get('gst', None)

    def post(self):
        # database connectivity code
        sdate = datetime.strptime(self.__startdate, "%d-%m-%Y")
        stimestamp = datetime.timestamp(sdate)
        edate = datetime.strptime(self.__enddate, "%d-%m-%Y")
        etimestamp = datetime.timestamp(edate)

        try:
            connection = mysql.connector.connect(host='localhost',
                                                 database='paynet-staging',
                                                 user='admin',
                                                 password='mysql')
            if connection.is_connected():

                mycursor = connection.cursor()

                class create_dict(dict):

                    def __init__(self):
                        pass

                    def add(self, key, value):
                        self[key] = value

                mydict = create_dict()

                query = "select t.transaction_id, t.time_stamp, t.user_id,u.merchant_id, u.name,t.card_type, " \
                        "t.amount, t.amount_credited, t.result,d.quantity,d.charge, " \
                        "case when t.result = '0001' then 'Merchant Id and password does not match' " \
                        "when t.result = '0002' then 'API Key not valid' " \
                        "when t.result = '0003' then 'Transaction ID not found' " \
                        "when t.result = '0004' then 'Unknown transaction error occurred' " \
                        "when t.result = '0' then 'Payment Rejected' " \
                        "when t.result = '1' then 'The payment is prepared' " \
                        "when t.result = '2' then 'PIN rejected, payment rejected' " \
                        "when t.result = '3' then 'PIN accepted, payment approved' " \
                        "when t.result = '4' then 'Signature was not uploaded, payment rejected' " \
                        "when t.result = '5' then 'Signature accepted, payment approved' " \
                        "when t.result = '6' then 'Payment Approved' " \
                        "when t.result = '66' then 'Payment Initiated' " \
                        "when t.result = '11' then 'Force Accept' " \
                        "when t.result = '12' then 'Refunded' " \
                        "when t.result = '50' then 'Payment Rejected' " \
                        "when t.result = '800' then 'Payment Rejected' " \
                        "when t.result = 'REJECTED' then 'Rejected' " \
                        "when t.result = 'FAILURE' then 'Not Successfull' " \
                        "when t.result = 'SUCCESS' then 'Successfull' " \
                        "when t.result = 'PENDING' then 'Pending From Bank' " \
                        "when t.result = ' ' then 'Not Completed' " \
                        "when t.result = 'default' then 'Not Successfull' " \
                        "end as transaction_status from `paynet-staging`.transaction as t, " \
                        "user as u, merchant_deductable as d " \
                        " where t.user_id = u.user_id and u.merchant_id = d.merchant_id " \
                        " and u.merchant_id = '" + str(self.__merchant_id) + \
                        "' AND t.time_stamp >= '" + str(stimestamp) + "' AND t.time_stamp <='" + str(etimestamp) + "'"

                mycursor.execute(query)
                myresult = mycursor.fetchall()

                for row in myresult:

                    if row[11] == 'Payment Approved':
                        deduct_percentage_charge = float(row[6]) * (float(self.__percentage)/100.0)
                        print("charge", deduct_percentage_charge)
                        if deduct_percentage_charge > float(self.__flat_amount):
                            deduct_charge = deduct_percentage_charge
                        else:
                            deduct_charge = float(self.__flat_amount)

                        gst = deduct_charge * (float(self.__gst) /(100.0+float(self.__gst)))
                        print("gst", gst)

                        actual_paynet_charge = deduct_charge - gst
                    else:
                        actual_paynet_charge = 0.0
                        gst = 0.0

                    mydict.add(row[0], ({"Transaction id": row[0], "Timestamp": str(row[1]), "User id": row[2],
                                         "Merchant id": row[3], "Merchant name": row[4], "Card type": row[5],
                                         "Amount": str(row[6]), "Amount credited": str(row[7]), "Result": row[8],
                                         "Quantity": row[9], "Charge": str(row[10]), "Transaction Status": row[11],
                                         "Paynet Charge":"{:.2f}".format(actual_paynet_charge),
                                         "GST":"{:.2f}".format(gst)}))

                    '''print(len(mydict))
                    if len(mydict) > 0:
                        mydict.add("Response", ({"Response":"Success"}))
                    else:
                        mydict.add("Response", ({"Response":"Failure"}))
'''
                result_json = json.dumps(mydict, indent=2, sort_keys=True)
                result_json = json.loads(result_json)

                return result_json
        except Error as e:
            print("Error while connecting to MySQL", e)

        finally:
            if connection.is_connected():
                mycursor.close()
                connection.close()
                print("MySQL connection is closed")


# api.add_resource(CalculateCharges, '/CalculateCharges/')
api.add_resource(CalculateChargesGSTAPI,'/CalculateChargesGSTAPI/')

if __name__ == "__main__":
    app.run(debug=True)
