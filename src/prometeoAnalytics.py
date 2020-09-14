import pandas as pd
import numpy as np
import sqlalchemy
import json
import os

# Constants / definitions (not used as mutable variables)
CONST_sensor_log_tablename : Final = 'firefighter_sensor_log'
CONST_analytics_tablename : Final = 'firefighter_status_analytics'
CONST_firefighter_id_col : Final = 'firefighter_id'
CONST_firefighter_id_col_type : Final = sqlalchemy.types.VARCHAR(length=20) # mySQL needs to be told this explicitly in order to generate correct SQL
CONST_timestamp_col : Final = 'timestamp_mins'


class prometeoAnalytics(object):

    # Get configuration (1) currenty supported/active gases and (2) gas exposure windows and limits (e.g. from AEGL).
    def __init__(self):
        with open('windows_and_limits_config.json', 'r') as file:
            self.windows_and_limits = json.load(file)
            file.close()
        with open('supported_gases_config.json', 'r') as file:
            self.supported_gases = json.load(file)
            file.close()

        # db identifiers
        SQLALCHEMY_DATABASE_URI = ("mysql+pymysql://"+os.getenv('MARIADB_USER')
                                    +":"+os.getenv("MARIADB_PASSWORD")
                                    +"@"+os.getenv("MARIADB_HOST")
                                    +":"+int(os.getenv("MARIADB_PORT"))
                                    +"/prometeo")

        metadata=sqlalchemy.MetaData(SQLALCHEMY_DATABASE_URI)
        self.db_engine = metadata.bind

    # Query the last 8 hours of sensor logs
    def get_block_of_sensor_readings(window_end, from_db=True) :
        
        # very important: everything in the system needs to synchronise to minute-boundaries
        window_end = window_end.floor(freq='min')
        longest_window = max([window['mins'] for window in self.windows_and_limits])
        window_start = window_end - pd.Timedelta(minutes = longest_window) # e.g. 8hrs ago

        sensor_log_df = pd.DataFrame()
        # Get from database
        if from_db :
            # Three ways to think about, depending on how time is being reported and whether this is running 'live' or in testing mode
            # 1. Get a RANGE
            # todo: check if this is inclusive or right/left exclusive - may be the source of the 'last TWA calculation of the day' issue?
            sql = "SELECT * FROM " + CONST_sensor_log_tablename + " where " + CONST_timestamp_col + " between '" + window_start.isoformat() + "' and '" + window_end.isoformat() + "'"
            # 2. Get the LAST 8 HOURS (using the app server's perception of 'now')
            # sql = "SELECT * FROM " + CONST_sensor_log_tablename + " where " + CONST_timestamp_col + " > '" + window_start.isoformat() + "'"
            # 3. Get the LAST 8 HOURS (using the database's perception of 'now')
            # sql = "SELECT * FROM " + CONST_sensor_log_tablename + " where " + CONST_timestamp_col + " > (DATE_SUB(NOW(), INTERVAL 8 HOUR))
            sensor_log_df = pd.read_sql_query(sql, self.db_engine, parse_dates=[CONST_timestamp_col], index_col=CONST_timestamp_col).sort_index()

        # Get from local CSV files - useful when testing (using local CSV data from the February test)
        else :
            global sensor_log_from_csv_df
            if 'sensor_log_from_csv_df' not in globals() :
                print('Reading CSV files from disk...')
                day1_df = pd.read_csv('~/code/prometeo/js/2020-02-10 data prescribed burn V2_FORMAT.csv', parse_dates=[CONST_timestamp_col], index_col = CONST_timestamp_col)
                day2_df = pd.read_csv('~/code/prometeo/js/2020-02-11 data prescribed burn V2_FORMAT.csv', parse_dates=[CONST_timestamp_col], index_col = CONST_timestamp_col)
                sensor_log_from_csv_df= pd.concat([day1_df, day2_df])

            print("Reading block of sensor readings *** from CSV ***")
            sensor_log_df = sensor_log_from_csv_df.loc[window_start:window_end,:].sort_index().copy()

        if (sensor_log_df.empty) : print("No 'live' sensor records found in range ["+str(window_start)+" to "+str(window_end)+"]") # todo: write to a logfile somewhere?
        return sensor_log_df


    # Given up to 8 hours of data, calculates the time-weighted average and limit gauge (%) for all firefighters, for all supported gases, for all configured time periods.
    # sensor_log_chunk_df  : A time-indexed dataframe covering up to 8 hours of sensor data for all firefighters, for all supported gases. Requires firefighterID and supported gases as columns.
    # self.windows_and_limits   : A list detailing every supported time-window over which to calcuate the time-weighted average (label, number of minutes and gas limit gauges for each window).
    # current_timestamp    : The timstamp for which to calculate time-weighted averages.
    def calculate_TWA_and_gauge_for_all_firefighters(sensor_log_chunk_df, self.windows_and_limits, current_timestamp) :

        # We'll be processing the windows in descending order of length (mins) 
        windows_in_descending_order = sorted([window for window in self.windows_and_limits], key=lambda window: window['mins'], reverse=True)
        longest_window_mins = windows_in_descending_order[0]['mins'] # topmost element in the windows
        longest_window_timedelta = pd.Timedelta(minutes = longest_window_mins)
        slice_correction = pd.Timedelta(minutes = 1) # subtract 1 min because pandas index slicing is __inclusive__ and we don't want 11 samples in a 10 min average
        timestamp_correction = pd.Timedelta(minutes = 1)

        longest_window_df = sensor_log_chunk_df[(current_timestamp - longest_window_timedelta + slice_correction).isoformat():current_timestamp.isoformat()]

        # todo: Obtain information about long dropouts (>3mins) before resampling. Each TWA will need this to confirm there's enough info for calculating that TWA.
        # todo: long-dropout detection goes here
        
        
        # To calculate time-weighted averages, every time-slice in the window is quantized to equal 1-minute lengths.
        # (it can be done with 'ragged' / uneven time-slices, but the code is more complex and hence error-prone, so we use 1-min quantization as standard here).
        # We still have to fill-in missing minutes (e.g. while a device was offline).
        # When a sensor value isn't known for any *short* period (e.g. 3mins), we'll assume that the value observed at the end of that period is
        # a reasonable approximation of its value during that period. i.e. we 'backfill' those gaps. (note: On the US standard
        # websites (OSHA, NIOSH), it seems that only sampling every 10 mins or even every hour is common. So backfilling a small number of
        # missing minutes seems a reasonable strategy)

        # Resample the data to 1 minute boundaries, grouped by firefighter and backfilling any missing minutes
        resample_timedelta = pd.Timedelta(minutes = 1)
        longest_window_cleaned_df = (longest_window_df
                                    .sort_index()
                                    .groupby(CONST_firefighter_id_col, group_keys=False)
                                    .resample(resample_timedelta).backfill(limit=3)
                                    )
        
        # We're about to calculate stats for a number of different time-windows and then join them together.
        # To do so, we need a common timestamp/key to merge on - same as the sensor records timestamp/key (assuming there are any).
        # Since records are quantized to floor(minute), they need a 1 minute arrival buffer, hence this correction.
        common_key = (current_timestamp - timestamp_correction).floor(freq='min')


        
        
        # Now, get the 'latest' sensor readings - may or may not be available, depending on dropouts
        latest_sensor_readings = [] # store for merging later on
        if (common_key in longest_window_cleaned_df.index) :
            # If there's a 'latest' sensor reading available, get it. Lots of info here isn't used for analytics and
            # will need to be merged back into the final dataframe. (The analytics only needs the supported gases and
            # firefighter_id). Fields that aren't used for analytics include some sensor fields (e.g. humidity, 
            # temperature, ...) and many non-sensor fields (e.g. device_id, device_battery_level, ...)
            latest_sensor_readings_df = (longest_window_cleaned_df
                                        .loc[[common_key],:] # the current timeslice
                                        .reset_index()
                                        .set_index([CONST_firefighter_id_col, CONST_timestamp_col])  # match up the indexing to the TWA dataframes
                                        )
            # Store for merging later on
            latest_sensor_readings = [latest_sensor_readings_df] 
        else : 
            print (common_key.isoformat() + " No 'live' sensor records found. Calculating Time-Weighted Averages...") # todo: logfile?
        
        # Now iterate over the time windows, calculate their time-weighted averages & limit gauge %, and merge them to a common dataframe
        calculations_for_all_windows = [] # list of results from each window, for merging at the end
        for window in windows_in_descending_order :
            window_mins = window['mins']
            window_timedelta = pd.Timedelta(minutes = window_mins)
            window_duration_label = window['label']
            
            # get a slice for this specific window, for all supported gas sensor readings (and excluding anll other columns)
            analytic_cols = self.supported_gases + [CONST_firefighter_id_col]
            window_df = (longest_window_cleaned_df
                        .loc[(current_timestamp - window_timedelta + slice_correction).isoformat():current_timestamp.isoformat(), analytic_cols])
            
            # If the window is empty, we still need to append it to the 'everything' dataframe
            if (window_df.empty) :
                # # Update column titles - add the time period over which we're averaging, so we can merge dataframes later without column name conflicts
                # empty_df = window_df.reset_index().set_index([CONST_firefighter_id_col, CONST_timestamp_col])
                # empty_twa_df = empty_df.add_suffix('_twa_' + window_duration_label)
                # empty_gauge_df = empty_df.add_suffix('_gauge_' + window_duration_label)
                # # Now save the results from this time window as a single merged dataframe (TWAs and Limit Gauges)
                # calculations_for_all_windows.append(pd.concat([empty_twa_df, empty_gauge_df], axis='columns'))
                continue # TODO: can we get away with just leaving the empty window out? Depends on how the DB write will work

            # Sanity check that there's never more data in the window than there should be (1 record per minute per FF, max)
            assert(window_df.groupby(CONST_firefighter_id_col).size().max() <= window_mins)

            # Confirm there's enough info to calculate with, using information about gaps obtained before resampling
            # todo: check here whether to calculate or return NaN, based on max length of dropouts / disconnected periods.
            
            
            
            # Calculate time-weighted average exposure
            window_sample_count = window_timedelta / resample_timedelta
            # Use .sum() and divide by a fixed-time denominator for each window.
            # Don't use .mean() - it has a variable denominator (however many datapoints it happens to have), which over-estimates exposure during startup.
            window_twa_df = (window_df.groupby(CONST_firefighter_id_col).sum() / float(window_sample_count))
            # Give the window its index key. Note: since records are quantized to floor(minute), they need a 1 minute arrival buffer, hence the correction
            window_twa_df[CONST_timestamp_col] = common_key

            # Prepare the results for limit gauges and merging
            window_twa_df = window_twa_df.reset_index().set_index([CONST_firefighter_id_col, CONST_timestamp_col])
            
            # Calculate gas limit gauge - percentage over / under the calculated TWA values
            # (must compare gases in the same order as the dataframe columns)
            limits_in_column_order = [float(window['gas_limits'][gas]) for gas in window_twa_df.columns if gas in self.supported_gases]
            window_gauge_df = (window_twa_df * 100 / limits_in_column_order).round(0).astype(int) # whole integer percentage, shorter to send over JSON

            # Update column titles - add the time period over which we're averaging, so we can merge dataframes later without column name conflicts
            window_twa_df = window_twa_df.add_suffix('_twa_' + window_duration_label)
            window_gauge_df = window_gauge_df.add_suffix('_gauge_' + window_duration_label)

            # Now save the results from this time window as a single merged dataframe (TWAs and Limit Gauges)
            calculations_for_all_windows.append(pd.concat([window_twa_df, window_gauge_df], axis='columns'))

        # Merge latest sensors readings with TWAs and Gauges from all time windows - so we have 'everything' for this time step
        everything_for_1_min_df = pd.concat(latest_sensor_readings + calculations_for_all_windows, axis='columns')

        # If there were no latest sensors readings to merge, then just set all the sensor cols to null
        if not latest_sensor_readings :
            # Do this for all sensor columns, except the two keys
            for col in list(set(sensor_log_chunk_df.columns) - set([CONST_firefighter_id_col, CONST_timestamp_col])) :
                everything_for_1_min_df[col] = None # todo: works, but would prefer a more "pandas-y" way of achieving this with a multi-level index...
        
        # makes it slightly easier to read
        everything_for_1_min_df = everything_for_1_min_df[sorted(everything_for_1_min_df.columns.to_list(), key=str.casefold)]
        
        return everything_for_1_min_df


    # This is 'main' - runs all of the core analytics for Prometeo in a given minute.
    # time : The datetime for which to calculate sensor analytics. Defaults to 'now'.
    # from_db : Utility flag for unit testing - defaults to running against the database.
    #           Setting from_db=False causes tests to be run against local CSV files instead.
    # commit : Utility flag for unit testing - defaults to committing analytic results to
    #          the database. Setting commit=False prevents unit tests from writing to the database.
    def run_analytics (time=pd.Timestamp.now(), from_db=True, commit=True) :

        # Read the max window block from the database - todo: ensure this a non-blocking read (not read-for-update)
        # We can make this more performant, but at the start "make it correct, then write the tests, THEN optimise (with a safety net)" 
        # Also - this is robust to dropouts - querying 'everything known' from the sensor log ensures that
        # analytic processing can include any delayed records that have since arrived.
        sensor_log_df = get_block_of_sensor_readings(time, from_db)

        # Stop if there's no data (e.g. (1) after the system is booted but before any records have come in. (2) 8+ hours after an event
        if (sensor_log_df.empty) : return    
        
        # Work out all the time-weighted averages and corresponding limit gauges for all firefighters, all limits and all gases.
        analytics_df = calculate_TWA_and_gauge_for_all_firefighters(sensor_log_df, self.windows_and_limits, time)

        if commit :
            analytics_df.to_sql(CONST_analytics_tablename, self.db_engine, if_exists='append', dtype={CONST_firefighter_id_col:CONST_firefighter_id_col_type})

        # dicts are easy to return as JSON too
        # analytics_df_dict = analytics_df.reset_index().to_json(orient='records', indent=2)
        
        return analytics_df
