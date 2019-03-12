from flask_restful import Resource, Api
from flask_cors import CORS
from flask import Flask, jsonify, request

from pymongo import MongoClient
from pymongo import errors

from bson import ObjectId
import json
import urllib.parse

app = Flask(__name__)
CORS(app)
api = Api(app)

def mongoClient(uri, db_name, collection_name):
    client = MongoClient(uri)
    db = client[db_name]
    collection = db[collection_name]
    return collection

class eth_radar_demo(Resource):
    def get(self):
        #uri = 'mongodb://longhashdba:longhash123QAZ@47.96.228.84:27017/data_service'
        collection = mongoClient('localhost', 'parity', 'radar_demo')
        info = collection.find({'address':request.args.get('query')}).limit(1)
       
        for dic in info:
            del dic['_id']
            
        return dic
    
class stableCoin_dailyFlow_demo(Resource):
    def get(self):
        
        uri = 'mongodb://root:' + urllib.parse.quote('longhash123!@#QAZ') + '@47.96.228.84'
        collection = mongoClient(uri, 'parity', 'stableCoin_dailyFlow')
        
        info = collection.find({'name':request.args.get('query')}).sort([('blk',-1)]).limit(1)
       
        for dic in info:
            del dic['_id']
           
        return dic['data']

api.add_resource(eth_radar_demo, '/eth_radar_demo/')
api.add_resource(stableCoin_dailyFlow_demo, '/stableCoin_dailyFlow_demo/')

if __name__ == '__main__':
    app.run(host='0.0.0.0', port=8003, debug=True, threaded=True)
    
    