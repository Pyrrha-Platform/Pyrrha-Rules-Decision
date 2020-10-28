import json
import os
import numpy as np
import pandas as pd
import sqlalchemy
import logging


# Constants / definitions

# Database constants
SENSOR_LOG_TABLE = 'firefighter_sensor_log'
ANALYTICS_TABLE = 'firefighter_status_analytics'
FIREFIGHTER_ID_COL = 'firefighter_id'
# mySQL needs to be told the firefighter_id column type explicitly in order to generate correct SQL.
FIREFIGHTER_ID_COL_TYPE = sqlalchemy.types.VARCHAR(length=20)
TIMESTAMP_COL = 'timestamp_mins'
# Normally the 'analytics' LED color will be the same as the 'device' LED color, but in a disconnected scenario, they
# may be different. We want to capture both. 
STATUS_LED_COL = 'analytics_status_LED'
TWA_SUFFIX = '_twa'
GAUGE_SUFFIX = '_gauge'
MIN_SUFFIX = '_%smin'
GREEN = 1
YELLOW = 2
RED = 3
RANGE_EXCEEDED = -1

# Cache Constants
DATA_START = 'data_start'
DATA_END = 'data_end'
WINDOW_START = 'window_start'
WINDOW_END = 'window_end'
OVERLAP_MINS = 'overlap_mins'
PROPORTION_OF_WINDOW = 'proportion_of_window'

# Status constants - percentages that define green/red status (yellow is the name of a configuration parameter)
GREEN_RANGE_START = 0
RED_RANGE_START = 99
RED_RANGE_END = RED_RANGE_START * 1000 # Can be arbitrarily large, as the next range bound is np.inf.

# Configuration constants - for reading values from config files.
DEFAULT_CONFIG_FILENAME = 'prometeo_config.json'
WINDOWS_AND_LIMITS_PROPERTY = 'windows_and_limits'
WINDOW_MINS_PROPERTY = 'mins'
SUPPORTED_GASES_PROPERTY = 'supported_gases'
YELLOW_WARNING_PERCENT_PROPERTY = 'yellow_warning_percent'
SAFE_ROUNDING_FACTORS_PROPERTY = 'safe_rounding_factors'
GAS_LIMITS_PROPERTY = 'gas_limits'
AUTOFILL_MINS_PROPERTY = 'autofill_missing_sensor_logs_up_to_N_mins'

# Sensor range limitations. These are intentionally hard-coded and not configured. They're used
# to 1. Cross-check that the PPM limits configured for each time-window respects the sensitivity
# range of the sensors and 2. Check when sensor values have gone out of range.
SENSOR_RANGE_PPM  = {
    'carbon_monoxide'  : {'min' : 1   , 'max' : 1000}, # CJMCU-4541 / MICS-4514 Sensor
    'nitrogen_dioxide' : {'min' : 0.05, 'max' : 10  }  # CJMCU-4541 / MICS-4514 Sensor
}


class GasExposureAnalytics(object):


    # Validate the configuration - log helpful error messages if invalid.
    def _validate_config(self, config_filename) :

        valid_config = True # "Trust, but verify" ;-)
        critical_config_issues = []

        # Check that all configured windows cover the same set of gases (i.e. that the first window covers the same set of gases as all other windows)
        # Note: Set operations are valid for .keys() views [https://docs.python.org/3.8/library/stdtypes.html#dictionary-view-objects]
        mismatched_configs_idx = [idx for idx, window in enumerate(self.WINDOWS_AND_LIMITS) if (window[GAS_LIMITS_PROPERTY].keys() != self.WINDOWS_AND_LIMITS[0][GAS_LIMITS_PROPERTY].keys())]
        mismatched_configs = []
        if mismatched_configs_idx :
            mismatched_configs = [self.WINDOWS_AND_LIMITS[0]]
            mismatched_configs += [self.WINDOWS_AND_LIMITS[idx] for idx in mismatched_configs_idx]
            valid_config = False
            message = "%s : The '%s' for every time-window must cover the same set of gases - but these have mis-matches %s" \
                % (config_filename, GAS_LIMITS_PROPERTY, mismatched_configs)
            self.logger.critical(message)
            critical_config_issues += [message]

        # Check that the supported gases are covered by the configuration        
        if not set(self.SUPPORTED_GASES).issubset(self.WINDOWS_AND_LIMITS[0][GAS_LIMITS_PROPERTY].keys()) :
            valid_config = False
            message = "%s : One or more of the '%s' %s has no limits defined in '%s' %s." \
                % (config_filename, SUPPORTED_GASES_PROPERTY, str(self.SUPPORTED_GASES), WINDOWS_AND_LIMITS_PROPERTY, str(list(self.WINDOWS_AND_LIMITS[0][GAS_LIMITS_PROPERTY].keys())))
            self.logger.critical(message)
            critical_config_issues += [message]

        # For each supported gas, check that limits PPM configuration is within that sensor's range.
        # The limits must be less than the range of the sensor. For best reporting, the range should be at least
        # two or more times the upper limit and a warning will be produced if this is not the case. To to illustrate
        # why: Say a firefighter experiences [30mins at 1ppm. Then 30mins at 25ppm] and the 1hr limit is 10ppm.  Then
        # one hour into the fire, this firefighter has experienced an average of 13ppm per hour, well over the 10ppm
        # limit - their status should be ‘Red’. However, if the range of the sensor is 0-10ppm, then the command center
        # would actually see their status as *Range Exceeded* (not Red, Yellow or Green), which is not very helpful.
        # It's essentially saying "this firefighter's average exposure is unknown - it may be OK or it may not.
        # Prometeo can't tell, because the sensors aren't sensitive enough for these conditions". For the firefighter
        # to get an accurate report, this sensor would need to have a range of at least 25ppm (and likely more),
        # so that exposure could be accurately measured and averaged to 13ppm.
        # (Note: the sensor returns *Range Exceeded* to prevent incorrect PPM averages from being calculated.
        #        e.g. in the above scenario, we do not want to incorrectly calculate an average of 5.5ppm (Green) from a
        #        sensor showing 30mins at 1ppm and 30mins at 10ppm, the max the sensor can 'see').
        for gas in self.SUPPORTED_GASES :
            limits = [window[GAS_LIMITS_PROPERTY][gas] for window in self.WINDOWS_AND_LIMITS]
            if ( (min(limits) < SENSOR_RANGE_PPM[gas]['min']) or (max(limits) > SENSOR_RANGE_PPM[gas]['max']) ) : 
                valid_config = False
                message = ("%s : One or more of the '%s' configurations %s is incompatible with the range of the '%s' sensor (min: %s, max: %s).") \
                           % (config_filename, GAS_LIMITS_PROPERTY, limits, gas, SENSOR_RANGE_PPM[gas]['min'], SENSOR_RANGE_PPM[gas]['max'])
                self.logger.critical(message)
                critical_config_issues += [message]
            if ((max(limits)*2) > SENSOR_RANGE_PPM[gas]['max']) : 
                # This is valid, but not optimal. Produce a warning.
                message = ("%s : One or more of the '%s' configurations %s is very close to the range of the '%s' sensor (min: %s, max: %s)." +
                           "\nSensors shoud have a much larger range than the limits - e.g. 2x at a minimum .") \
                           % (config_filename, GAS_LIMITS_PROPERTY, limits, gas, SENSOR_RANGE_PPM[gas]['min'], SENSOR_RANGE_PPM[gas]['max'])
                self.logger.warning(message)

        # Check there's a valid definition of yellow - should be a percentage between 1 and 99
        if not ( (self.YELLOW_WARNING_PERCENT > 0) and (self.YELLOW_WARNING_PERCENT < 100) ) :
            valid_config = False
            message = "%s : '%s' should be greater than 0 and less than 100 (percent), but is %s" \
                % (config_filename, YELLOW_WARNING_PERCENT_PROPERTY, self.YELLOW_WARNING_PERCENT)
            self.logger.critical(message)
            critical_config_issues += [message]

        # For each supported gas, check there's a valid factor defined for safe rounding - should be a positive integer.
        for gas in self.SUPPORTED_GASES :
            if  ( (not isinstance(self.SAFE_ROUNDING_FACTORS[gas], int)) or (not (self.SAFE_ROUNDING_FACTORS[gas] >= 0) ) ) :
                valid_config = False
                message = "%s : '%s' for '%s' should be a positive integer, but is %s" \
                    % (config_filename, SAFE_ROUNDING_FACTORS_PROPERTY, gas, self.SAFE_ROUNDING_FACTORS[gas])
                self.logger.critical(message)
                critical_config_issues += [message]
        
        # Check the max number of auto-filled minutes is a positive integer.
        if  ( (not isinstance(self.AUTOFILL_MINS, int)) or (not (self.AUTOFILL_MINS >= 0) ) ) :            
            valid_config = False
            message = "%s : '%s' should be a positive integer, but is %s" \
                % (config_filename, AUTOFILL_MINS_PROPERTY, self.AUTOFILL_MINS)
            self.logger.critical(message)
            critical_config_issues += [message]
        elif (self.AUTOFILL_MINS > 20) :
            # Recommended (but not enforced) to be less than 20 mins.
            warning = "%s : '%s' is not recommended to be more than 20 minutes, but is %s" \
                % (config_filename, AUTOFILL_MINS_PROPERTY, self.AUTOFILL_MINS)
            self.logger.warning(warning)

        assert valid_config, ''.join([('\nCONFIG ISSUE (%s) : %s' % (idx+1, issue)) for idx, issue in enumerate(critical_config_issues)])

        return


    # Create an instance of the Prometeo Gas Exposure Analytics, initialising it with a data source and an appropriate
    # configuration file.
    # list_of_csv_files : Use the supplied CSV files as sensor data instead of the Prometeo DB, so that tests can test
    #                     against a known data. This option should not be used at runtime.
    # config_filename   : Allow overriding TWA time-window configurations, so that tests can test against a known
    #                     configuration. This option should not be used at runtime, as prometeo uses a relational
    #                     database and the analytics table schema is static, not dynamic.
    def __init__(self, list_of_csv_files=None, config_filename=DEFAULT_CONFIG_FILENAME):

        self.logger = logging.getLogger('GasExposureAnalytics')

        # Get configuration
        with open(os.path.join(os.path.dirname(__file__), config_filename)) as file:
            configuration = json.load(file)
            file.close()

        # WINDOWS_AND_LIMITS   : A list detailing every supported time-window over which to calcuate the time-weighted
        #   average (label, number of minutes and gas limit gauges for each window) - e.g. from NIOSH, ACGIH, EU-OSHA.
        self.WINDOWS_AND_LIMITS = configuration[WINDOWS_AND_LIMITS_PROPERTY]
        # SUPPORTED_GASES   : The list of gases that Prometeo devices currently have sensors for.
        #   To automatically enable analytics for new gases, simply add them to this list.
        self.SUPPORTED_GASES = configuration[SUPPORTED_GASES_PROPERTY]
        # YELLOW_WARNING_PERCENT : yellow is a configurable percentage - the status LED will go yellow when any gas 
        #   reaches that percentage (e.g. 80%) of the exposure limit for any time-window.
        self.YELLOW_WARNING_PERCENT = configuration[YELLOW_WARNING_PERCENT_PROPERTY]
        # SAFE_ROUNDING_FACTORS : Why round? Because each gas has a number of decimal places that are meaningful and
        #   beyond which extra digits are trivial. Rounding protects unit tests from brittleness due to these trivial 
        #   differences in computations. If a value changes by more than 1/10th of the smallest unit of the
        #   most-sensitive gas, then we want to know (e.g. fail a test), any less than that and the change is negligible.
        #   e.g.: At time of writing, Carbon Monoxide had a range of 0 to 420ppm and Nitrogen Dioxide, had a range
        #   of 0.1 to 10ppm. So the safe rounding factors for these gases would be 1 decimal place for CO and 2 for NO2.
        self.SAFE_ROUNDING_FACTORS = configuration[SAFE_ROUNDING_FACTORS_PROPERTY]

        # AUTOFILL_MINS: A buffer of N mins (e.g. 10 mins) during which the system will assume any missing data just
        #                means a device is disconnected and the data is temporarily delayed. It will 'treat' the
        #                missing data (e.g. by substituting an average). After this number of minutes of missing
        #                sensor data, the system will stop estimating and assume the firefighter has powered  off their
        #                device and left the event.
        self.AUTOFILL_MINS = configuration[AUTOFILL_MINS_PROPERTY]

        # Cache of 'earliest and latest observed data points for each firefighter'. Necessary for the AUTOFILL_MINS
        # functionality.
        self._FF_TIME_SPANS_CACHE = None

        # Validate the configuration - log helpful error messages if invalid.
        self._validate_config(config_filename)

        # db identifiers
        SQLALCHEMY_DATABASE_URI = ("mysql+pymysql://"+os.getenv('MARIADB_USERNAME')
                                    +":"+os.getenv("MARIADB_PASSWORD")
                                    +"@"+os.getenv("MARIADB_HOST")
                                    +":"+str(os.getenv("MARIADB_PORT"))
                                    +"/prometeo")
        metadata=sqlalchemy.MetaData(SQLALCHEMY_DATABASE_URI)
        self._db_engine = metadata.bind


        # By default, the analytics will run from a database.
        self._from_db = True

        # For testing, the analytics can also be run from a set of CSV files.
        if list_of_csv_files is not None : 
            self._from_db = False

            self.logger.info("Taking sensor readings *** from CSV ***")
            # Allow clients to pass either single (non-list) CSV file path, or a list of CSV file paths
            if not isinstance(list_of_csv_files, list) : list_of_csv_files = [list_of_csv_files]
            dataframes = []
            for csv_file in list_of_csv_files : 
                df = pd.read_csv(csv_file, engine='python', parse_dates=[TIMESTAMP_COL], index_col = TIMESTAMP_COL)
                assert FIREFIGHTER_ID_COL in df.columns, "CSV files is missing key columns %s" % (required_cols)
                dataframes.append(df)
            # Merge the dataframes (also pre-sort, to speed up test runs and enable debug slicing on the index)
            self._sensor_log_from_csv_df = pd.concat(dataframes).sort_index()


    # Query the last N hours of sensor logs, where N is the longest configured time-window length. As with all methods
    # in this class, sensor data is assumed to be keyed on the floor(minute) timestamp when it was captured - i.e.
    # a sensor value captured at 12:00:05 is stored against a timestamp of 12:00:00.
    # block_end : The datetime from which to look back when reading the sensor logs (e.g. 'now').
    def _get_block_of_sensor_readings(self, block_end) :
        
        # Get the start of the time block to read - i.e. the end time, minus the longest window we're interested in.
        # Add 1 min 'correction' to the start times because both SQL 'between' and Pandas slices are *in*clusive and we
        # don't want (e.g.) 61 samples in a 60 min block.
        one_minute = pd.Timedelta(minutes = 1)
        longest_block = max([window['mins'] for window in self.WINDOWS_AND_LIMITS])
        block_start = block_end - pd.Timedelta(minutes = longest_block) + one_minute # e.g. 8hrs ago

        message = ("Reading sensor log in range [%s to %s]" % (block_start.isoformat(), block_end.isoformat()))
        if not self._from_db : message += " (local CSV file mode)"
        self.logger.info(message)

        sensor_log_df = pd.DataFrame()
        ff_time_spans_df = None
        if self._from_db :
            # Get from database with a non-blocking read (this type of SELECT is non-blocking on
            # MariaDB/InnoDB - ref: https://dev.mysql.com/doc/refman/8.0/en/innodb-consistent-read.html)
            sql = ("SELECT * FROM " + SENSOR_LOG_TABLE + " where " + TIMESTAMP_COL
                    + " between '" + block_start.isoformat() + "' and '" + block_end.isoformat() + "'")
            sensor_log_df = (pd.read_sql_query(sql, self._db_engine,
                                              parse_dates=[TIMESTAMP_COL], index_col=TIMESTAMP_COL))

        else :
            # Get from local CSV files - useful when testing (e.g. using known sensor test data)
            sensor_log_df = self._sensor_log_from_csv_df.loc[block_start:block_end,:].copy()

        if (sensor_log_df.empty) :
            self.logger.info("No 'live' sensor records found in range [%s to %s]"
                             % (block_start.isoformat(), block_end.isoformat()))
            # Reset the cache of 'earliest and latest observed data points for each firefighter'.
            # If we didn't do this, firefighters 'data time span' would stretch over multiple days. We want
            # it to reset once there's been no data within the longest configured time-window.
            self._FF_TIME_SPANS_CACHE = None

        else : 
            # sort is required for several operations, e.g. slicing, re-sampling, etc. Do it once, up-front.
            sensor_log_df = sensor_log_df.sort_index()

            # Update the cache of 'earliest and latest observed data points for each firefighter'. As firefighters come
            # online (and as data comes in after an outage), each new chunk may contain records for firefighters that
            # are not yet captured in the cache.
            # [DATA_START]: the earliest observed data point for each firefighter - grows as firefighters
            #                 join an event (and different for each Firefighter)
            # [DATA_END]  : the latest observed data point for each firefighter so far - a moving target, but
            #                 fixed for *this* chunk of data (and potentially different for each Firefighter)
            ff_time_spans_in_this_block_df = (pd.DataFrame(sensor_log_df.reset_index()
                                                .groupby(FIREFIGHTER_ID_COL)
                                                [TIMESTAMP_COL].agg(['min', 'max']))
                                                .rename(columns = {'min':DATA_START, 'max':DATA_END}))
            if self._FF_TIME_SPANS_CACHE is None :
                # First-time cache creation
                self._FF_TIME_SPANS_CACHE = pd.DataFrame(ff_time_spans_in_this_block_df)
            else :  
                # Update the earliest and latest observed timestamp for each firefighter.
                # note: use pd.merge() not pd.concat() - concat drops the index names causing later steps to crash
                self._FF_TIME_SPANS_CACHE = pd.merge(np.fmin(ff_time_spans_in_this_block_df.loc[:, DATA_START],
                                                             self._FF_TIME_SPANS_CACHE.loc[:, DATA_START]),
                                                     np.fmax(ff_time_spans_in_this_block_df.loc[:, DATA_END],
                                                             self._FF_TIME_SPANS_CACHE.loc[:, DATA_END]),
                                                     how='outer', on=FIREFIGHTER_ID_COL)

            # Take a working copy of the cache, so we can manupulate it during analytic processing.
            ff_time_spans_df = self._FF_TIME_SPANS_CACHE.copy()

            # Add a buffer of N mins (e.g. 10 mins) to the 'data end'. The system will assume up to this
            # many minutes of missing data just means a device is disconnected and the data is temporarily delayed.
            # It will 'treat' the missing data (e.g. by substituting an average). After this number of minutes of
            # missing sensor data, the system will stop estimating and assume the firefighter has powered 
            # off their device and left the event.
            ff_time_spans_df.loc[:, DATA_END] += pd.Timedelta(minutes = self.AUTOFILL_MINS)

        return sensor_log_df, ff_time_spans_df


    # Given up to 8 hours of data, calculates the time-weighted average and limit gauge (%) for all firefighters, for
    # all supported gases, for all configured time periods.
    # sensor_log_chunk_df: A time-indexed dataframe covering up to 8 hours of sensor data for all firefighters,
    #                      for all supported gases. Requires firefighterID and supported gases as columns.
    # ff_time_spans_df   : A dataset containing the 'earliest and latest observed data points for each 
    #                      firefighter'. Necessary for the AUTOFILL_MINS functionality.
    # timestamp_key :    The minute-quantized timestamp key for which to calculate time-weighted averages.
    def _calculate_TWA_and_gauge_for_all_firefighters(self, sensor_log_chunk_df, ff_time_spans_df, timestamp_key) :

        # We'll be processing the windows in descending order of length (mins) 
        windows_in_desc_mins_order = sorted([w for w in self.WINDOWS_AND_LIMITS], key=lambda w: w['mins'], reverse=True)
        longest_window_mins = windows_in_desc_mins_order[0]['mins'] # topmost element in the ordered windows

        # Get sensor records for the longest time-window. Note: we add 1 min to the start-time, because slicing
        # is *in*clusive and we don't want N+1 samples in an N min block of sensor records.
        one_minute = pd.Timedelta(minutes = 1)
        longest_window_start = timestamp_key - pd.Timedelta(minutes=longest_window_mins) + one_minute
        longest_window_df = sensor_log_chunk_df.loc[longest_window_start:timestamp_key, :]

        # It's essential to know when a sensor value can't be trusted - i.e. when it has exceeded its range (signalled
        # by the value '-1'). When this happens, we need to replace that sensor's value with something that
        # both (A) identifies it as untrustworthy and (B) also causes calculated values like TWAs and Gauges to be
        # similarly identified. That value is infinity (np.inf). To to illustrate why: Say a firefighter experiences
        # [30mins at 1ppm. Then 30mins at 25ppm] and the 1 hour limit is 10ppm.  Then 1 hour into the fire, this
        # firefighter has experienced an average of 13ppm per hour, well over the 10ppm limit - their status should be
        # ‘Red’. However, if the range of the sensor were 0-10ppm, then at best, the sensor could only provide [30mins
        # at 1ppm. Then 30mins at 10ppm], averaging to 5.5ppm per hour which is *Green* (not Red or even Yellow).  To
        # prevent this kind of under-reporting, the device sends '-1' to indicate that the sensor has exceeded its
        # range and we substitute that with infinity (np.inf), which then flows correctly through the time-weighted
        # average calculations.
        longest_window_df.loc[:, self.SUPPORTED_GASES] = (longest_window_df.loc[:, self.SUPPORTED_GASES].mask(
                                                   cond=(longest_window_df.loc[:, self.SUPPORTED_GASES] < 0),
                                                   other=np.inf))

        # To calculate time-weighted averages, every time-slice in the window is quantized ('resampled') to equal
        # 1-minute lengths. (it can be done with 'ragged' / uneven time-slices, but the code is more complex and
        # hence error-prone, so we use 1-min quantization as standard here). The system is expected to provide data
        # that meets this requirement, so this step is defensive. We don't backfill missing entries here.
        #
        # (note: the double sort_index() here looks odd, but it seems both necessary and fairly low cost:
        # 1. Resampling requires the original index to be sorted, reasonably enough. 2. The resampled dataframe
        # can't be sliced by date index unless it's sorted too. However these 'extra' sorts don't seem to carry
        # a noticeable performance penalty, possibly since the original dataframe is sorted to begin with)
        resample_timedelta = pd.Timedelta(minutes = 1)
        longest_window_cleaned_df = (longest_window_df
                                    .sort_index()
                                    .groupby(FIREFIGHTER_ID_COL, group_keys=False)
                                    .resample(resample_timedelta).nearest(limit=1)
                                    .sort_index())
        
        # Before doing the main work, save a copy of the data for each device at 'timestamp_key' *if* available
        # (may not be, depending on dropouts). Note: this is the first of several chunks of data that we will
        # later merge on the timestamp_key.
        latest_device_data = []
        if (timestamp_key in longest_window_cleaned_df.index) :
            # If there's data for a device at 'timestamp_key', get a copy of it. While some if it is used for
            # calculating average exposures (e.g. gases, times, firefighter_id), much of it is not (e.g. temperature,
            # humidity, battery level) and this data needs to be merged back into the final dataframe.
            latest_sensor_readings_df = (longest_window_cleaned_df
                                        .loc[[timestamp_key],:] # the current minute
                                        .reset_index()
                                        .set_index([FIREFIGHTER_ID_COL, TIMESTAMP_COL])  # key to merge on at the end
                                        )
            # Store in a list for merging later on
            latest_device_data = [latest_sensor_readings_df] 
        else : 
            message = "No 'live' sensor records found at timestamp %s. Calculating Time-Weighted Averages anyway..."
            self.logger.info(message % (timestamp_key.isoformat()))
        
        # Now the main body of work - iterate over the time windows, calculate their time-weighted averages & limit
        # gauge percentages. Then merge all of these bits of info back together (with the original device data) to
        # form the overall analytic results dataframe.
        calculations_for_all_windows = [] # list of results from each window, for merging at the end
        for time_window in windows_in_desc_mins_order :
            
            # Get the relevant slice of the data for this specific time-window, for all supported gas sensor readings
            # (and excluding all other columns)
            window_mins = time_window['mins']
            window_length = pd.Timedelta(minutes = window_mins)
            window_start = timestamp_key - window_length + one_minute
            analytic_cols = self.SUPPORTED_GASES + [FIREFIGHTER_ID_COL]
            window_df = longest_window_cleaned_df.loc[window_start:timestamp_key, analytic_cols]

            # If the window is empty, then there's nothing to do, just move on to the next window
            if (window_df.empty) :
                continue

            # Check that there's never more data in the window than there should be (max 1 record per min, per FF)
            assert(window_df.groupby(FIREFIGHTER_ID_COL).size().max() <= window_mins)

            # Calculate time-weighted average exposure for this time-window.
            # A *time-weighted* average, means each sensor reading is multiplied by the length of time the reading
            # covers, before dividing by the total time covered. This can get very complicated if readings are unevenly
            # spaced or if they get lost, or sent late due to connectivity dropouts. So Prometeo makes two design
            # choices that account for these issues, and simplify calculations (reducing opportunities for error).
            # (1) The system takes exactly one reading per minute, no more & no less, so the multiplication factor for 
            #     every reading is always 1.
            # (2) Any missing/lost sensor readings are approximated by using the average value for that sensor over the
            #     time-window in question. (Care needs to be taken to ensure that calculations don't inadvertently 
            #     approximate them as '0ppm').
            # Since the goal we're after here is to get the average over a time-window, we don't need to actually 
            # fill-in the missing entries, we can just get the average of the available sensor readings.
            window_twa_df = window_df.groupby(FIREFIGHTER_ID_COL).mean()

            # The average alone is not enough, we also have to adjust it to reflect how much of the time-window the
            # data represents. e.g. Say the 8hr time-weighted average (TWA) exposure limit for CO exposure is 27ppm.
            # Now imagine we observe 30ppm in the first 15 mins of an event and then have a connectivity dropout for
            # the next 15 mins. What do we show the command center user? It's over the limit for an 8hr average, but
            # we're only 30mins into that 8-hour period. So we adjust the TWA to the proportion of the time window
            # that has actually elapsed. Note: this implicitly assumes that firefighter exposure is zero before the
            # first recorded sensor value and after the last recorded value.

            # To work out the window proportion, we (A) calculate the time overlap between the moving window and the
            # available data timespans for each Firefighter, then (B) Divide the overlap by the total length of the
            # time-window to get the proportion. Finally (C) Multiply the TWAs for each firefighter by the proportion
            # for that firefighter.

            # (A.1) Get the available data timespans for each Firefighter, only selecting firefighters that are in
            # this window. (take a copy so that data from one window does not contaminate the next)
            ffs_in_this_window = window_df.loc[:, FIREFIGHTER_ID_COL].unique()
            overlap_df = ff_time_spans_df.loc[ff_time_spans_df.index.isin(ffs_in_this_window), :].copy()

            # (A.2) Add on the moving window timespans (note: no start correction here because it's not a slice.
            overlap_df = overlap_df.assign(**{WINDOW_START : timestamp_key - window_length, WINDOW_END : timestamp_key})            

            # (A.3) Calculate the overlap between the moving window and the available data timespans for each Firefighter.
            # overlap = (earliest_end_time - latest_start_time). Negative overlap is meaningless, so when it happens,
            # treat it as zero overlap.
            overlap_delta = (overlap_df.loc[:, [DATA_END,WINDOW_END]].min(axis='columns')
                             - overlap_df.loc[:, [DATA_START,WINDOW_START]].max(axis='columns'))
            overlap_df.loc[:, OVERLAP_MINS] = (overlap_delta.transform(
                lambda delta: delta.total_seconds()/float(60) if delta.total_seconds() > 0 else 0))

            # (B) Divide the overlap by the total length of the time-window to get a proportion. Maximum overlap is 1.
            overlap_df.loc[:, PROPORTION_OF_WINDOW] = (overlap_df.loc[:, OVERLAP_MINS].transform(
                lambda overlap_mins : overlap_mins/float(window_mins) if overlap_mins < window_mins else 1))

            # (C) Multiply the TWAs for each firefighter by the proportion for that firefighter.
            # Also apply rounding at this point.
            window_twa_df = window_twa_df.multiply(overlap_df.loc[:, PROPORTION_OF_WINDOW], axis='rows')
            for gas in self.SUPPORTED_GASES : 
                window_twa_df.loc[:, gas] = np.round(window_twa_df.loc[:, gas], self.SAFE_ROUNDING_FACTORS[gas])
            
            # Prepare the results for limit gauges and merging
            window_twa_df = (window_twa_df
                            .assign(**{TIMESTAMP_COL: timestamp_key})
                            .reset_index()
                            .set_index([FIREFIGHTER_ID_COL, TIMESTAMP_COL]))
            
            # Calculate gas limit gauge - percentage over / under the calculated TWA values
            # (force gases and limits to have the same column order as each other before comparing)
            gas_limits = [float(time_window[GAS_LIMITS_PROPERTY][gas]) for gas in self.SUPPORTED_GASES]
            window_gauge_df = ((window_twa_df.loc[:, self.SUPPORTED_GASES] * 100 / gas_limits)
                                .round(0)) # we don't need decimal precision for percentages

            # Update column titles - add the time period over which we're averaging, so we can merge dataframes later
            # without column name conflicts.
            window_twa_df = window_twa_df.add_suffix((TWA_SUFFIX + MIN_SUFFIX) % (str(time_window[WINDOW_MINS_PROPERTY])))
            window_gauge_df = window_gauge_df.add_suffix((GAUGE_SUFFIX + MIN_SUFFIX) % (str(time_window[WINDOW_MINS_PROPERTY])))

            # Now save the results from this time window as a single merged dataframe (TWAs and Limit Gauges)
            calculations_for_all_windows.append(pd.concat([window_twa_df, window_gauge_df], axis='columns'))

        # Merge 'everything' for this time step - TWAs & Gauges from all time windows, latest sensors readings, ...
        everything_for_1_min_df = pd.concat(latest_device_data + calculations_for_all_windows, axis='columns')

        # If there were no latest sensors readings to merge, then just set all the sensor cols to null (np.nan)
        if not latest_device_data :
            sensor_cols = list(set(longest_window_df.columns) - set([FIREFIGHTER_ID_COL, TIMESTAMP_COL]))
            everything_for_1_min_df = everything_for_1_min_df.assign(**{col:np.NaN for col in sensor_cols})

        # Now that we have all the informatiom, we can determine the overall Firefighter status.
        # Green/Red status boundaries are constant, yellow is configurable. If a sensor exceeded its range, then the
        # Firefighter's status cannot be accurately determined (and the Gauge value will be np.inf)
        yellow_range_start = self.YELLOW_WARNING_PERCENT - 1
        everything_for_1_min_df[STATUS_LED_COL] = pd.cut(
            everything_for_1_min_df.filter(like=GAUGE_SUFFIX).max(axis='columns'),
            bins=[GREEN_RANGE_START, yellow_range_start, RED_RANGE_START, RED_RANGE_END, np.inf], include_lowest=True,
            labels=[GREEN,YELLOW,RED,RANGE_EXCEEDED])

        # Use the Prometeo constant for 'out-of-range sensor value' rather than np.inf from here on.
        # (np.inf is useful for the math, but not for communicating / storing / displaying).
        # Here we convert np.inf values in gas readings, TWAs and Gauges to a Prometeo constant.
        gas_cols = everything_for_1_min_df.columns[everything_for_1_min_df.columns.str.contains("|".join(self.SUPPORTED_GASES))]
        everything_for_1_min_df.loc[:, gas_cols] = (everything_for_1_min_df.loc[:, gas_cols]
                                              .fillna(value=np.nan)
                                              .replace(np.inf, RANGE_EXCEEDED))

        # Make the dataframe easier to print/read/debug 
        col_headers_sorted_for_readability = sorted(everything_for_1_min_df.columns.to_list(), key=str.casefold)
        everything_for_1_min_df = everything_for_1_min_df.loc[:, col_headers_sorted_for_readability]
        
        return everything_for_1_min_df


    # This is 'main' - runs all of the core analytics for Prometeo in a given minute.
    # time : The datetime for which to calculate sensor analytics. Defaults to 'now'.
    # commit : Utility flag for unit testing - defaults to committing analytic results to
    #          the database. Setting commit=False prevents unit tests from writing to the database.
    def run_analytics (self, current_timestamp=pd.Timestamp.now(), commit=True) :

        # Very important: All sensor records are keyed on the FF id and the minute in which they arrive. So if 'now'
        # is 08:10:11 (11s past 8.10am) then there's another 49s to go before we can expect all the similarly-keyed
        # (08:10:00) sensor records to have arrived. Hence the actual 'latest' data that we're interested in running
        # analytics for is "any data keyed 08:09:00" i.e. (now.floor() minus 1 minute) - that 1 minute is the arrival
        # buffer for the data.
        timestamp_key = current_timestamp.floor(freq='min') - pd.Timedelta(minutes = 1)

        message = ("Running Prometeo Analytics for minute key '%s'" % (timestamp_key.isoformat()))
        if not self._from_db : message += " (local CSV file mode)"
        self.logger.info(message)

        # Read a block of sensor logs from the DB, covering the longest window we're calculating over (usually 8hrs).
        # Note: This has the advantage of always including all known sensor data, even when that data was delayed due
        # to loss of connectivity. That makes the 'right now' limit detection as good quality as it can be... at the
        # cost of the DB reads not being as efficient as they could be. (e.g. it would be more efficient to read 48
        # previously-calculated 10-min TWAs and average *those*, instead of averaging 480 raw 1-min sensor logs
        # (a word of caution - the average of an average is NOT automatically a valid average, care needs to be taken
        # with denominators...). This would be more efficient, but poorer quality, because the 'right now' limit
        # detection would be based on derived values containing assumptions about missing data). So for now, we
        # prioritise quality and resist "premature optimisation/efficiency" at least until the system is
        # sound / correct, after which optimisation tradeoffs can be prioritised as needed.
        sensor_log_df, ff_time_spans_df = self._get_block_of_sensor_readings(timestamp_key)

        # Stop if there's no data (e.g. (1) after the system is booted but before any records have come in. (2) 8+ hours after an event
        if (sensor_log_df.empty) : return
        
        # Work out all the time-weighted averages and corresponding limit gauges for all firefighters, all limits and all gases.
        analytics_df = self._calculate_TWA_and_gauge_for_all_firefighters(sensor_log_df, ff_time_spans_df, timestamp_key)

        if commit :
            analytics_df.to_sql(ANALYTICS_TABLE, self._db_engine, if_exists='append', dtype={FIREFIGHTER_ID_COL:FIREFIGHTER_ID_COL_TYPE})

        return analytics_df
