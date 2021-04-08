import os
from flask import Flask, Response, jsonify, abort
from flask_restplus import Api, Resource, fields, reqparse
from flask_cors import CORS, cross_origin
import json
import pandas as pd
from .GasExposureAnalytics import GasExposureAnalytics
from dotenv import load_dotenv
import time
import atexit
from apscheduler.schedulers.background import BackgroundScheduler
import logging
import sqlalchemy
import sys
from flask import request
from werkzeug.exceptions import HTTPException

# get logging level from the environment, default to INFO
logging.basicConfig(level=os.environ.get("LOGLEVEL", logging.INFO))

# Get a logger and keep its name in sync with this filename
logger = logging.getLogger(os.path.basename(__file__))

# The application
app = Flask(__name__)
CORS(app)

logger.info('starting application')

# On Bluemix, get the port number from the environment variable PORT
# When running this app on the local machine, default to 8080
port = int(os.getenv('PORT', 8080))

# DB Connections and identifier constants
SQLALCHEMY_DATABASE_URI = ("mysql+pymysql://"+os.getenv('MARIADB_USERNAME')
                            +":"+os.getenv("MARIADB_PASSWORD")
                            +"@"+os.getenv("MARIADB_HOST")
                            +":"+str(os.getenv("MARIADB_PORT"))
                            +"/prometeo")
DB_ENGINE = sqlalchemy.MetaData(SQLALCHEMY_DATABASE_URI).bind
ANALYTICS_TABLE = 'firefighter_status_analytics'
FIREFIGHTER_ID_COL = 'firefighter_id'
TIMESTAMP_COL = 'timestamp_mins'
STATUS_LED_COL = 'analytics_status_LED'

# We initialize the prometeo Analytics engine.
perMinuteAnalytics = GasExposureAnalytics()



# Calculates Time-Weighted Average exposures and exposure-limit status 'gauges' for all firefighters for the last minute.
def callGasExposureAnalytics():
    logger.info('Running analytics')

    # Run all of the core analytics for Prometeo for a given minute.
    status_updates_df = perMinuteAnalytics.run_analytics()

    # # TODO: Pass all status details and gauges on to the dashboard via an update API
    # status_updates_json = None # Information available for the current minute (may be None)
    # if status_updates_df is not None:
    #     status_updates_json = (status_updates_df.reset_index(TIMESTAMP_COL) # index json by firefighter only
    #                            .to_json(orient='index', date_format='iso')) # send json to dashboard 
    #
    # resp = requests.post(API_URL, json=status_updates_json)
    # if resp.status_code != EXPECTED_RESPONSE_CODE:
    #     logger.error(f'ERROR: dashboard update API error code [{resp.status_code}]')
    #     logger.debug(f'\t with JSON: {status_updates_json}')


# Start up a scheduled job to run once per minute
ANALYTICS_FREQUENCY_SECONDS = 60
scheduler = BackgroundScheduler()
scheduler.add_job(func=callGasExposureAnalytics, trigger="interval", seconds=ANALYTICS_FREQUENCY_SECONDS)
scheduler.start()
# Shut down the scheduler when exiting the app
atexit.register(lambda: scheduler.shutdown())


@app.route('/health', methods=['GET'])
def health():
    return "healthy"

# The ENDPOINTS
@app.route('/get_status', methods=['GET'])
def getStatus():
 
    try:
        firefighter_id = request.args.get(FIREFIGHTER_ID_COL)
        timestamp_mins = request.args.get(TIMESTAMP_COL)

        print('entering /get_status')

        # Return 404 (Not Found) if the record IDs are invalid
        if (firefighter_id is None) or (timestamp_mins is None):
            logger.error('Missing parameters : '+FIREFIGHTER_ID_COL+' : '+str(firefighter_id)
                            +', '+TIMESTAMP_COL+' : '+str(timestamp_mins))
            abort(404)

        # Read the requested Firefighter status
        sql = ('SELECT '+FIREFIGHTER_ID_COL+', '+TIMESTAMP_COL+', '+STATUS_LED_COL+' FROM '+ANALYTICS_TABLE+
            ' WHERE '+FIREFIGHTER_ID_COL+' = "'+firefighter_id+'" AND '+TIMESTAMP_COL+' = "'+timestamp_mins+'"')

        logger.info('entering GET status')
        logger.info(sql)
        firefighter_status_df = pd.read_sql_query(sql, DB_ENGINE)

        logger.info('/get_status called!')
        logger.info(sql)

        # Return 404 (Not Found) if no record is found
        if (firefighter_status_df is None) or (firefighter_status_df.empty):
            logger.error('No status found for : ' + FIREFIGHTER_ID_COL + ' : ' + str(firefighter_id)
                             + ', ' + TIMESTAMP_COL + ' : ' + str(timestamp_mins))
            abort(404)
        else:
            firefighter_status_json = (firefighter_status_df
                                    .rename(columns={STATUS_LED_COL: "status"}) # name as expected by client
                                    .iloc[0,:] # convert dataframe to series (should never be more than 1 record)
                                    .to_json(date_format='iso'))
            return firefighter_status_json
    except HTTPException as e:
        logger.error(f'{e}')
        raise e
    except Exception as e:
        # Return 500 (Internal Server Error) if there's any unexpected errors.
        logger.error(f'Internal Server Error: {e}')
        abort(500)

# The ENDPOINTS
@app.route('/get_status_details', methods=['GET'])
def getStatusDetails():

    try:
        firefighter_id = request.args.get(FIREFIGHTER_ID_COL)
        timestamp_mins = request.args.get(TIMESTAMP_COL)

        # Return 404 (Not Found) if the record IDs are invalid
        if (firefighter_id is None) or (timestamp_mins is None):
            logger.error('Missing parameters : '+FIREFIGHTER_ID_COL+' : '+str(firefighter_id)
                            +', '+TIMESTAMP_COL+' : '+str(timestamp_mins))
            abort(404)

        # Read the requested Firefighter status
        sql = ('SELECT * FROM '+ANALYTICS_TABLE+
            ' WHERE '+FIREFIGHTER_ID_COL+' = "'+firefighter_id+'" AND '+TIMESTAMP_COL+' = "'+timestamp_mins+'"')
        firefighter_status_df = pd.read_sql_query(sql, DB_ENGINE)

        # Return 404 (Not Found) if no record is found
        if (firefighter_status_df is None) or (firefighter_status_df.empty):
            logger.error('No status found for : ' + FIREFIGHTER_ID_COL + ' : ' + str(firefighter_id)
                             + ', ' + TIMESTAMP_COL + ' : ' + str(timestamp_mins))
            abort(404)
        else:
            firefighter_status_json = (firefighter_status_df
                                    .rename(columns={STATUS_LED_COL: "status"}) # name as expected by client
                                    .iloc[0,:] # convert dataframe to series (should never be more than 1 record)
                                    .to_json(date_format='iso'))
            return firefighter_status_json
    except HTTPException as e:
        logger.error(f'{e}')
        raise e
    except Exception as e:
        # Return 500 (Internal Server Error) if there's any unexpected errors.
        logger.error(f'Internal Server Error: {e}')
        abort(500)

@app.route('/get_configuration', methods=['GET'])
def getConfiguration():

    try:
        configuration = perMinuteAnalytics.CONFIGURATION
        # Return 404 (Not Found) if the configuration doesn't exist
        if (configuration is None):
            logger.error('getConfiguration: No configuration found.')
            abort(404)
        else:
            configuration_json = json.dumps(configuration)
            return configuration_json

    # Log and propagate HTTP exceptions.
    except HTTPException as e:
        logger.error(f'{e}')
        raise e

    except Exception as e:
        # Return 500 (Internal Server Error) if there's any unexpected errors.
        logger.error(f'Internal Server Error: {e}')
        abort(500)


if __name__ == '__main__':
    app.run(host='0.0.0.0', port=8080, debug=False)  # deploy with debug=False
