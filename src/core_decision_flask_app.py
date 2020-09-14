import os
from flask import Flask, Response, jsonify
from flask_restplus import Api, Resource, fields, reqparse
from flask_cors import CORS, cross_origin
import pandas as pd
from GasExposureAnalytics import GasExposureAnalytics


# The application
app = Flask(__name__)
CORS(app)

api_prometeo_analytics = Api(app, version='1.0', title="Calculates Time-Weighted Average exposures and exposure-limit status 'gauges' for all firefighters for the last minute.", validate=False)
ns = api_prometeo_analytics.namespace('GasExposureAnalytics', 'Calculates core Prometeo analytics')

# The API does not require any input data. Once called, it will retrieve the latest data from the database.
model_input = api_prometeo_analytics.model('Enter the data:', {'todo': fields.String(description='todo')})

# On Bluemix, get the port number from the environment variable PORT
# When running this app on the local machine, default to 8080
port = int(os.getenv('PORT', 8080))

# We initialize the prometeo Analytics engine.
perMinuteAnalytics = GasExposureAnalytics()


# The ENDPOINT
@ns.route('/prometeo_analytics')

class prometeo_analytics(Resource):
    @api_prometeo_analytics.response(200, "Success", model_input)
    @api_prometeo_analytics.expect(model_input)
    def post(self):
        # We prepare the arguments
        parser = reqparse.RequestParser()
        parser.add_argument('firefighter_ids', type=list)
        args = parser.parse_args()

        # Run all of the core analytics for Prometeo for a given minute.
        limits_and_gauges_for_all_firefighters_df = perMinuteAnalytics.run_analytics()

        # Return the limits and status gauges as json
        if limits_and_gauges_for_all_firefighters_df is None :
            return None
        
        return limits_and_gauges_for_all_firefighters_df.to_json(orient='index')
        
        

if __name__ == '__main__':
    app.run(host='0.0.0.0', port=port, debug=False)  # deploy with debug=False
