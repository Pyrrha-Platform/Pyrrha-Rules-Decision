from flask import Flask, Response, jsonify
from flask_restplus import Api, Resource, fields, reqparse
from flask_cors import CORS, cross_origin
import os
from prometeoAnalytics import prometeoAnalytics


# The application
app = Flask(__name__)
CORS(app)

api_prometeo_analytics = Api(app, version='1.0', title="Calculates Time-Weighted Average exposures and exposure-limit status 'gauges' for all firefighters for the last minute.", validate=False)
ns = api_prometeo_analytics.namespace('prometeoAnalytics', 'Calculates core Prometeo analytics')

# The API does not require any input data, but may optionally accept a list of Firefighter ID's as prompts to check for recently-received sensor records.
model_input = api_prometeo_analytics.model('Enter the data:', {'firefighter_ids': fields.List(fields.String(description='Firefighter IDs'))})

# On Bluemix, get the port number from the environment variable PORT
# When running this app on the local machine, default to 8080
port = int(os.getenv('PORT', 8080))

# We initialize the prometeo Analytics engine.
perMinuteAnalytics = prometeoAnalytics()


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
        limits_and_gauges = perMinuteAnalytics.run_analytics(list(args["firefighter_ids"]))
        # limits_and_gauges_df, limits_and_gauges_json = perMinuteAnalytics.run_analytics(list(args["firefighter_ids"]))
        
        # # We return the status in a json format
        # return jsonify({"Status": status})

if __name__ == '__main__':
    app.run(host='0.0.0.0', port=port, debug=False)  # deploy with debug=False
