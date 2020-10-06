import os
import unittest

import pandas as pd
import numpy as np
import logging

import src
from src import GasExposureAnalytics

# ---------------------------------------

# DATASET FOR TESTING
TEST_DATA_CSV_FILEPATH = os.path.join(os.path.dirname(__file__), 'GasExposureAnalytics_test_dataset.csv')
ANALYTIC_CONFIGURATION_FOR_THIS_TEST = os.path.join(os.path.dirname(__file__), 'GasExposureAnalytics_test_config.json')

# FIELD / COLUMN / VALUE NAMES
FIREFIGHTER_ID_COL = 'firefighter_id'
TIMESTAMP_COL = 'timestamp_mins'
CARBON_MONOXIDE_COL = 'carbon_monoxide'
NITROGEN_DIOXIDE_COL = 'nitrogen_dioxide'
STATUS_LED_COL = 'analytics_status_LED'
TWA_SUFFIX = '_twa'
GAUGE_SUFFIX = '_gauge'
MIN_SUFFIX = '_%smin'
WINDOW_MINS_PROPERTY = 'mins'

# STATUS
GREEN = 1
YELLOW = 2
RED = 3
RANGE_EXCEEDED = -1
STATUS_UNAVAILABLE = np.NaN
STATUS_LABEL = {GREEN: 'Green', YELLOW: 'Yellow', RED: 'Red', RANGE_EXCEEDED: 'Sensor Range Exceeded', STATUS_UNAVAILABLE: 'Unavailable'}

# ---------------------------------------

logging.basicConfig(level=logging.INFO) # set to logging.INFO if you want to see all the routine INFO messages

# Unit tests for the GasExposureAnalytics class.
class GasExposureAnalyticsTestCase(unittest.TestCase):

    # Load a known sensor log, so that we can calculate gas exposure analytics for
    #  it and test that they match expectations. (not doing this in setUp because
    # we don't want to re-load for every test method)
    _analytics_test = GasExposureAnalytics(TEST_DATA_CSV_FILEPATH, config_filename=ANALYTIC_CONFIGURATION_FOR_THIS_TEST)

    def setUp(self):
        pass

    def tearDown(self):
        pass

    # Core utility method for testing :
    # * Takes in a mocked 'now' timestamp, a firefighter, a gas, the desired result fields to check (e.g. TWA or gauge).
    # * Calculates the gas results (TWAs and Gauges) for the firefighter at that time during the burn test dataset.
    # * Checks that the calculated results for all time-windows match the given 'expected' values.
    # * If there are any discrepancies, generates a detailed description for debugging.
    def _check_results_for_one_firefighter_one_gas(self, firefighter, timestamp_str, gas, result_fields,
                                                   expected_values, expected_status=None):

        # All sensor records are keyed on the minute in which they arrive. So if 'now' is 08:10:11 (11s past 8.10am)
        # then there's another 49s to go before we can expect all similarly-keyed (08:10:00) sensor records to have
        # arrived. Hence the 'latest' data that we're interested in getting analytics for is "any data keyed 08:09:00"
        # i.e. (now.floor() minus 1 minute) - that 1 minute is the arrival buffer for the data.
        now = pd.Timestamp(timestamp_str)
        analytic_timestamp_key = now - pd.Timedelta(minutes=1)
        
        # Calculate time-weighted average (TWA) exposure of the given firefighter to the given gas, at the given time
        # during the burn test dataset.
        result_df = self._analytics_test.run_analytics(now, commit=False)

        # Check that the calculated results for all TWA windows match the given 'expected' values.
        # 1. Check that there *is* a result.
        self.assertTrue((result_df is not None),
                        "No analytics results produced for any firefighter at '"+timestamp_str+"')")
        try :
            # 2. Reformat the result to make the next steps simpler.
            # Context: The TWA time-windows (10m, 30m, etc.) start to fall out of scope towards the end of an
            # event - e.g. there's no '10 min TWA' being produced 20 mins after an event ends, but there *are* still
            # 30min, 1hr, etc TWAs being produced.  So the reindex() here just inserts null values for any missing
            # TWAs - which allows subsequent code not to have to worry about the 'shape' of the results.
            test_key = (firefighter, analytic_timestamp_key)
            actual_values = (result_df.reindex(columns=result_fields).loc[test_key, result_fields].tolist())
            actual_status = (result_df.loc[test_key, STATUS_LED_COL]) # also check the final status assessment

        except KeyError :
            # If the results contain TWAs for other firefighters, but not the expected firefighter.
            self.assertTrue(False, "No analytics results available for ('"+firefighter+"', '"+timestamp_str+"')")

        # Check that the calculated results for all TWA windows match the given 'expected' values.
        test_failed = False
        for (result_field, expected_value, actual_value) in zip(result_fields, expected_values, actual_values) :
            # Check if both the expected and actual are NaNs, as we can't check (NaN == NaN) - not valid in python
            both_NaN = pd.isna([expected_value, actual_value]).all() 
            self.assertTrue((expected_value == actual_value) or (both_NaN),
                            (("%s expected to be %s but was %s "
                              +"\n\t(debug: %s - firefighter '%s' - expected: %s - actuals: %s)")
                             % (result_field, expected_value, actual_value, timestamp_str, firefighter,
                                expected_values, actual_values)))

        # Check that the overall calculated status matches the 'expected' status.
        both_statuses_NaN = pd.isna([expected_status, actual_status]).all()
        if expected_status is not None :
            self.assertTrue((expected_status == actual_status) or (both_statuses_NaN),
                            (("%s expected to be %s (%s) but was %s (%s) \n\t(debug: %s - firefighter '%s')")
                            % (STATUS_LED_COL, STATUS_LABEL[expected_status], expected_status,
                            STATUS_LABEL[actual_status], actual_status, timestamp_str, firefighter)))


    # Core utility method for testing TWAs (see _check_results_for_one_firefighter_one_gas for docs)
    def _check_twas_for_one_firefighter_one_gas(self, firefighter, timestamp_str, gas, expected_values) :
        # Get the field names for the various expected analytic results (dependent on windows & limits configuration)
        result_fields = [(gas + TWA_SUFFIX + MIN_SUFFIX) % (str(window[WINDOW_MINS_PROPERTY])) for window in self._analytics_test.WINDOWS_AND_LIMITS]
        self._check_results_for_one_firefighter_one_gas(
            firefighter, timestamp_str, gas, result_fields, expected_values)


    # Core utility method for testing Gauges (see _check_results_for_one_firefighter_one_gas for docs)
    def _check_gauges_for_one_firefighter_one_gas(self, firefighter, timestamp_str, gas, expected_values,
                                                  expected_status=None) :
        # Get the field names for the various expected analytic results (dependent on windows & limits configuration)
        result_fields = [(gas + GAUGE_SUFFIX + MIN_SUFFIX) % (str(window[WINDOW_MINS_PROPERTY])) for window in self._analytics_test.WINDOWS_AND_LIMITS]
        self._check_results_for_one_firefighter_one_gas(
            firefighter, timestamp_str, gas, result_fields, expected_values, expected_status)


    # Utility method for inspecting specifc firefighter metrics when writing unit tests, or debugging tests that fail.
    # Requires an analytic dataframe that contains all the computed analytics for a full burn event.
    @staticmethod
    def inspect_several_mins_for_one_firefighter(df, firefighter, timestamp_str, gas, pad_mins=10, col_match=None) :
        timeslice_start = pd.Timestamp(timestamp_str) - pd.Timedelta(minutes=pad_mins)
        timeslice_end = pd.Timestamp(timestamp_str) + pd.Timedelta(minutes=pad_mins)
        df = df.sort_index().loc[(firefighter, timeslice_start):(firefighter, timeslice_end),:]
        if col_match is not None :
            return df.filter(regex=gas + '.*' + col_match)
        else : 
            return df


    # #################################################################################
    #  SPECIFIC TESTS FOR SENSITIVE REGIONS OF AN EVENT
    #
    # e.g. just before the 1st sensor value, just after the 1st value, when a sensor drops out, when
    # there isn't enough data to safely give a particular TWA, etc...
    # #################################################################################


    def test_before_the_first_sensor_record_of_the_day(self):
        # The minute before the very first record of the day: no FF data yet, everything blank - assert empty
        pre_start_test_df = self._analytics_test.run_analytics(pd.Timestamp('2000-01-01 09:31:00'), commit=False)
        # Shouldn't return a dataframe
        assert pre_start_test_df is None, "Expected to find no sensor records before the event, but actually found "\
                                          + str(pre_start_test_df.index.size)

    def test_first_sensor_record_of_the_day_gas1(self):
        # The very first record of the day - 1 reading for 1 firefighter!
        self._check_twas_for_one_firefighter_one_gas('0007', '2000-01-01 09:33:00', CARBON_MONOXIDE_COL,
                                                     expected_values=[0.0, 0.0, 0.0, 0.0, 0.0])
    
    def test_first_sensor_record_of_the_day_gas2(self):
        # The very first record of the day - 1 reading for 1 firefighter!
        self._check_twas_for_one_firefighter_one_gas('0007', '2000-01-01 09:33:00', NITROGEN_DIOXIDE_COL,
                                                     expected_values=[0.0, 0.0, 0.0, 0.0, 0.0])


    def test_missing_some_sensor_readings_in_10min_window_gas1(self):
        # 10 min window for firefighter 0003 is only half full.
        self._check_twas_for_one_firefighter_one_gas('0003', '2000-01-01 15:03:00', CARBON_MONOXIDE_COL,
                                                     expected_values=[6.0, 6.3, 6.8, 9.6, 6.1])
    
    def test_missing_some_sensor_readings_in_10min_window_gas2(self):
        # 10 min window for firefighter 0003 is only half full.
        self._check_twas_for_one_firefighter_one_gas('0003', '2000-01-01 15:03:00', NITROGEN_DIOXIDE_COL,
                                                     expected_values=[0.3, 0.31, 0.34, 0.48, 0.3])


    def test_no_sensor_readings_in_10min_window_gas1(self):
        # 10 min window for firefighter 0003 is now empty.
        self._check_twas_for_one_firefighter_one_gas('0003', '2000-01-01 15:08:00', CARBON_MONOXIDE_COL,
                                                     expected_values=[np.nan, 6.2, 6.7, 9.7, 6.1])
    
    def test_no_sensor_readings_in_10min_window_gas2(self):
        # 10 min window for firefighter 0003 is now empty.
        self._check_twas_for_one_firefighter_one_gas('0003', '2000-01-01 15:08:00', NITROGEN_DIOXIDE_COL,
                                                     expected_values=[np.nan, 0.31, 0.33, 0.48, 0.31])


    def test_no_sensor_readings_in_30min_window_gas1(self):
        # As we head out past 30 mins after the last sensor reading, the 10m and 30m TWAs drop off
        self._check_twas_for_one_firefighter_one_gas('0003', '2000-01-01 15:28:00', CARBON_MONOXIDE_COL,
                                                     expected_values=[np.nan, np.nan, 4.2, 8.8, 6.1])
    
    def test_no_sensor_readings_in_30min_window_gas2(self):
        # As we head out past 30 mins after the last sensor reading, the 10m and 30m TWAs drop off
        self._check_twas_for_one_firefighter_one_gas('0003', '2000-01-01 15:28:00', NITROGEN_DIOXIDE_COL,
                                                     expected_values=[np.nan, np.nan, 0.21, 0.44, 0.31])


    def test_no_sensor_readings_in_30min_window_alt_gas1(self):
        # ...same as 'test_no_sensor_readings_in_30min_window' but with another FF (so it's not just '0003')
        self._check_twas_for_one_firefighter_one_gas('0010', '2000-01-01 14:00:00', CARBON_MONOXIDE_COL,
                                                     expected_values=[np.nan, np.nan, 4.4, 7.7, 3.9])
    
    def test_no_sensor_readings_in_30min_window_alt_gas2(self):
        # ...same as 'test_no_sensor_readings_in_30min_window' but with another FF (so it's not just '0003')
        self._check_twas_for_one_firefighter_one_gas('0010', '2000-01-01 14:00:00', NITROGEN_DIOXIDE_COL,
                                                     expected_values=[np.nan, np.nan, 0.22, 0.39, 0.19])


    def test_no_sensor_readings_in_60min_window_gas1(self):
        # 60 min window for firefighter 0003 is now empty.
        self._check_twas_for_one_firefighter_one_gas('0003', '2000-01-01 15:58:00', CARBON_MONOXIDE_COL,
                                                     expected_values=[np.nan, np.nan, np.nan, 6.5, 6.1])
    
    def test_no_sensor_readings_in_60min_window_gas2(self):
        # 60 min window for firefighter 0003 is now empty.
        self._check_twas_for_one_firefighter_one_gas('0003', '2000-01-01 15:58:00', NITROGEN_DIOXIDE_COL,
                                                     expected_values=[np.nan, np.nan, np.nan, 0.32, 0.31])

    def test_no_sensor_readings_in_4hr_window_gas1(self):
        # 4 hr window for firefighter 0003 is now empty.
        self._check_twas_for_one_firefighter_one_gas('0003', '2000-01-01 18:58:00', CARBON_MONOXIDE_COL,
                                                     expected_values=[np.nan, np.nan, np.nan, np.nan, 5.0])
    
    def test_no_sensor_readings_in_4hr_window_gas2(self):
        # 4 hr window for firefighter 0003 is now empty.
        self._check_twas_for_one_firefighter_one_gas('0003', '2000-01-01 18:58:00', NITROGEN_DIOXIDE_COL,
                                                     expected_values=[np.nan, np.nan, np.nan, np.nan, 0.25])


    def test_second_last_sensor_record_of_the_day_gas1(self):
        # The second-last TWA calculation of the day for '0008'
        self._check_twas_for_one_firefighter_one_gas('0008', '2000-01-01 22:48:00', CARBON_MONOXIDE_COL,
                                                     expected_values=[np.nan, np.nan, np.nan, np.nan, 0.2])
    
    def test_second_last_sensor_record_of_the_day_gas2(self):
        # The second-last TWA calculation of the day for '0008'
        self._check_twas_for_one_firefighter_one_gas('0008', '2000-01-01 22:48:00', NITROGEN_DIOXIDE_COL,
                                                     expected_values=[np.nan, np.nan, np.nan, np.nan, 0.01])


    def test_last_sensor_record_of_the_day_gas1(self):
        # The last TWA calculation of the day for '0008'
        self._check_twas_for_one_firefighter_one_gas('0008', '2000-01-01 22:49:00', CARBON_MONOXIDE_COL,
                                                     expected_values=[np.nan, np.nan, np.nan, np.nan, 0.2])
    
    def test_last_sensor_record_of_the_day_gas2(self):
        # The last TWA calculation of the day for '0008'
        self._check_twas_for_one_firefighter_one_gas('0008', '2000-01-01 22:49:00', NITROGEN_DIOXIDE_COL,
                                                     expected_values=[np.nan, np.nan, np.nan, np.nan, 0.01])


    def test_correct_twas_still_provided_when_live_sensor_is_offline_gas1(self):
        # Test a period when there are device connection dropouts, but the TWAs are OK
        self._check_twas_for_one_firefighter_one_gas('0006', '2000-01-01 12:05:00', CARBON_MONOXIDE_COL,
                                                     expected_values=[68.0, 117.8, 72.2, 21.5, 10.8])
    
    def test_correct_twas_still_provided_when_live_sensor_is_offline_and_gas2_sensor_range_exceeded(self):
        # Test a period when there are device connection dropouts, AND a gas sensor maxed out more than 10 mins ago.
        self._check_twas_for_one_firefighter_one_gas('0006', '2000-01-01 12:05:00', NITROGEN_DIOXIDE_COL,
                                                     expected_values=[3.4, RANGE_EXCEEDED, RANGE_EXCEEDED, RANGE_EXCEEDED, RANGE_EXCEEDED])


    def test_status_is_green_just_before_80pc_of_one_limit(self):
        # Test a period just before one TWA exceeds 80% of its limit (or whatever % is configured for 'yellow')
        self._check_gauges_for_one_firefighter_one_gas('0008', '2000-01-01 11:35:00', NITROGEN_DIOXIDE_COL,
                                                     expected_values=[19.0, 71.0, 57.0, 32.0, 16.0], expected_status=GREEN)

    def test_status_goes_yellow_just_after_80pc_of_one_limit(self):
        # Test a period just after one TWA exceeds 80% of its limit (or whatever % is configured for 'yellow')
        self._check_gauges_for_one_firefighter_one_gas('0008', '2000-01-01 11:45:00', NITROGEN_DIOXIDE_COL,
                                                     expected_values=[14.0, 87.0, 62.0, 38.0, 18.0], expected_status=YELLOW)
    
    def test_status_drops_back_to_green_after_yellow(self):
        # Test a period just after a yellow patch, followed by a green patch
        self._check_gauges_for_one_firefighter_one_gas('0008', '2000-01-01 12:05:00', NITROGEN_DIOXIDE_COL,
                                                     expected_values=[20.0, 74.0, 73.0, 52.0, 26.0], expected_status=GREEN)
    
    def test_status_is_yellow_just_before_one_limit_exceeded(self):
        # Test a period just before one TWA reaches its limit
        self._check_gauges_for_one_firefighter_one_gas('0008', '2000-01-01 13:22:00', NITROGEN_DIOXIDE_COL,
                                                     expected_values=[12.0, 49.0, 70.0, 94.0, 46.0], expected_status=YELLOW)

    def test_status_is_red_just_after_one_limit_exceeded(self):
        # Test a period just after one TWA reaches its limit
        self._check_gauges_for_one_firefighter_one_gas('0008', '2000-01-01 13:30:00', NITROGEN_DIOXIDE_COL,
                                                     expected_values=[21.0, 67.0, 76.0, 102.0, 50.0], expected_status=RED)

    def test_correct_twas_still_provided_when_live_sensor_is_offline_alt_gas1(self):
        # ...same as 'test_correct_twas_still_provided_when_live_sensor_is_offline' with another FF (not just '0006')
        self._check_twas_for_one_firefighter_one_gas('0010', '2000-01-01 12:05:00', CARBON_MONOXIDE_COL,
                                                     expected_values=[11.6, 12.2, 13.8, 4.5, 2.2])
    
    def test_correct_twas_still_provided_when_live_sensor_is_offline_alt_gas2(self):
        # ...same as 'test_correct_twas_still_provided_when_live_sensor_is_offline' with another FF (not just '0006')
        self._check_twas_for_one_firefighter_one_gas('0010', '2000-01-01 12:05:00', NITROGEN_DIOXIDE_COL,
                                                     expected_values=[0.58, 0.61, 0.69, 0.22, 0.11])


    def test_last_twa_of_the_day_gas1(self):
        # Just before the 8hr window trails off for the very last firefighter (0003), hours after their last sensor record.
        self._check_twas_for_one_firefighter_one_gas('0003', '2000-01-01 22:57:00', CARBON_MONOXIDE_COL,
                                                     expected_values=[np.nan, np.nan, np.nan, np.nan, 0.1])
    
    def test_last_twa_of_the_day_gas2(self):
        # Just before the 8hr window trails off for the very last firefighter (0003), hours after their last sensor record.
        self._check_twas_for_one_firefighter_one_gas('0003', '2000-01-01 22:57:00', NITROGEN_DIOXIDE_COL,
                                                     expected_values=[np.nan, np.nan, np.nan, np.nan, 0.01])


    def test_after_the_last_twa_of_the_day(self):
        # No more TWA data - everything run past the end of the 8hr longest window - assert empty
        post_end_test_df = self._analytics_test.run_analytics(pd.Timestamp('2000-01-01 22:58:00'), commit=False)

        # Doesn't return a dataframe (or shouldn't)
        assert post_end_test_df is None, "Expected to find no sensor records more than 8hrs after the event, but found "\
                                         + str(post_end_test_df.index.size)


    # #################################################################################
    #  GENERAL SAMPLING TESTS
    # #################################################################################


    def test_sample_point_0010_TWAs_gas1(self):
        # test TWA calculations for carbon monoxide
        self._check_twas_for_one_firefighter_one_gas('0001', '2000-01-01 11:24:00', CARBON_MONOXIDE_COL,
                                                     expected_values=[7.8, 2.8, 2.0, 0.6, 0.3])
    def test_sample_point_0010_TWAs_gas2(self):
        # test TWA calculations for nitrogen dioxide
        self._check_twas_for_one_firefighter_one_gas('0001', '2000-01-01 11:24:00', NITROGEN_DIOXIDE_COL,
                                                     expected_values=[0.39, 0.14, 0.1, 0.03, 0.02])

    def test_sample_point_0010_Gauges_gas1(self):
        # test Gauge calculations for carbon monoxide
        self._check_gauges_for_one_firefighter_one_gas('0001', '2000-01-01 11:24:00', CARBON_MONOXIDE_COL,
                                                     expected_values=[2.0, 2.0, 2.0, 2.0, 1.0], expected_status=GREEN)
    def test_sample_point_0010_Gauges_gas2(self):
        # test Gauge calculations for nitrogen dioxide
        self._check_gauges_for_one_firefighter_one_gas('0001', '2000-01-01 11:24:00', NITROGEN_DIOXIDE_COL,
                                                     expected_values=[8.0, 14.0, 10.0, 6.0, 4.0], expected_status=GREEN)

    def test_sample_point_0020_TWAs_gas1(self):
        # test TWA calculations for carbon monoxide
        self._check_twas_for_one_firefighter_one_gas('0010', '2000-01-01 11:30:00', CARBON_MONOXIDE_COL,
                                                     expected_values=[22.3, 11.7, 7.9, 2.4, 1.2])
    def test_sample_point_0020_TWAs_gas2(self):
        # test TWA calculations for nitrogen dioxide
        self._check_twas_for_one_firefighter_one_gas('0010', '2000-01-01 11:30:00', NITROGEN_DIOXIDE_COL,
                                                     expected_values=[1.12, 0.58, 0.39, 0.12, 0.06])

    def test_sample_point_0020_Gauges_gas1(self):
        # test Gauge calculations for carbon monoxide
        self._check_gauges_for_one_firefighter_one_gas('0010', '2000-01-01 11:30:00', CARBON_MONOXIDE_COL,
                                                     expected_values=[5.0, 8.0, 10.0, 7.0, 4.0], expected_status=GREEN)
    def test_sample_point_0020_Gauges_gas2(self):
        # test Gauge calculations for nitrogen dioxide
        self._check_gauges_for_one_firefighter_one_gas('0010', '2000-01-01 11:30:00', NITROGEN_DIOXIDE_COL,
                                                     expected_values=[22.0, 58.0, 39.0, 24.0, 12.0], expected_status=GREEN)

    def test_sample_point_0030_TWAs_gas1(self):
        # test TWA calculations for carbon monoxide
        self._check_twas_for_one_firefighter_one_gas('0003', '2000-01-01 11:15:00', CARBON_MONOXIDE_COL,
                                                     expected_values=[5.4, 6.1, 6.1, 2.8, 1.4])
    def test_sample_point_0030_TWAs_gas2(self):
        # test TWA calculations for nitrogen dioxide
        self._check_twas_for_one_firefighter_one_gas('0003', '2000-01-01 11:15:00', NITROGEN_DIOXIDE_COL,
                                                     expected_values=[0.27, 0.3, 0.31, 0.14, 0.07])

    def test_sample_point_0030_Gauges_gas1(self):
        # test Gauge calculations for carbon monoxide
        self._check_gauges_for_one_firefighter_one_gas('0003', '2000-01-01 11:15:00', CARBON_MONOXIDE_COL,
                                                     expected_values=[1.0, 4.0, 7.0, 8.0, 5.0], expected_status=GREEN)
    def test_sample_point_0030_Gauges_gas2(self):
        # test Gauge calculations for nitrogen dioxide
        self._check_gauges_for_one_firefighter_one_gas('0003', '2000-01-01 11:15:00', NITROGEN_DIOXIDE_COL,
                                                     expected_values=[5.0, 30.0, 31.0, 28.0, 14.0], expected_status=GREEN)


    def test_sample_point_0040_TWAs_gas1(self):
        # test TWA calculations for carbon monoxide
        self._check_twas_for_one_firefighter_one_gas('0003', '2000-01-01 11:35:00', CARBON_MONOXIDE_COL,
                                                     expected_values=[28.8, 15.4, 10.9, 4.5, 2.2])
    def test_sample_point_0040_TWAs_gas2(self):
        # test TWA calculations for nitrogen dioxide
        self._check_twas_for_one_firefighter_one_gas('0003', '2000-01-01 11:35:00', NITROGEN_DIOXIDE_COL,
                                                     expected_values=[1.44, 0.77, 0.55, 0.22, 0.11])

    def test_sample_point_0040_Gauges_gas1(self):
        # test Gauge calculations for carbon monoxide
        self._check_gauges_for_one_firefighter_one_gas('0003', '2000-01-01 11:35:00', CARBON_MONOXIDE_COL,
                                                     expected_values=[7.0, 10.0, 13.0, 14.0, 8.0], expected_status=GREEN)
    def test_sample_point_0040_Gauges_gas2(self):
        # test Gauge calculations for nitrogen dioxide
        self._check_gauges_for_one_firefighter_one_gas('0003', '2000-01-01 11:35:00', NITROGEN_DIOXIDE_COL,
                                                     expected_values=[29.0, 77.0, 55.0, 44.0, 22.0], expected_status=GREEN)


    def test_sample_point_0050_TWAs_gas1(self):
        # test TWA calculations for carbon monoxide
        self._check_twas_for_one_firefighter_one_gas('0006', '2000-01-01 10:35:00', CARBON_MONOXIDE_COL,
                                                     expected_values=[44.9, 24.4, 12.2, 3.0, 1.5])
    def test_sample_point_0050_TWAs_gas2(self):
        # test TWA calculations for nitrogen dioxide
        self._check_twas_for_one_firefighter_one_gas('0006', '2000-01-01 10:35:00', NITROGEN_DIOXIDE_COL,
                                                     expected_values=[RANGE_EXCEEDED, RANGE_EXCEEDED, RANGE_EXCEEDED, RANGE_EXCEEDED, RANGE_EXCEEDED])

    def test_sample_point_0050_Gauges_gas1(self):
        # test Gauge calculations for carbon monoxide
        self._check_gauges_for_one_firefighter_one_gas('0006', '2000-01-01 10:35:00', CARBON_MONOXIDE_COL,
                                                     expected_values=[11.0, 16.0, 15.0, 9.0, 6.0]) # note: will be 'RED' due to NO2
    def test_sample_point_0050_Gauges_gas2(self):
        # test Gauge calculations for nitrogen dioxide when the nitrogen dioxide sensor is maxed out within the last 10 mins.
        self._check_gauges_for_one_firefighter_one_gas('0006', '2000-01-01 10:35:00', NITROGEN_DIOXIDE_COL,
                                                     expected_values=[RANGE_EXCEEDED, RANGE_EXCEEDED, RANGE_EXCEEDED, RANGE_EXCEEDED, RANGE_EXCEEDED], expected_status=RANGE_EXCEEDED)


    def test_sample_point_0060_TWAs_gas1(self):
        # test TWA calculations for carbon monoxide
        self._check_twas_for_one_firefighter_one_gas('0007', '2000-01-01 12:05:00', CARBON_MONOXIDE_COL,
                                                     expected_values=[0.4, 0.4, 0.2, 0.1, 0.0])
    def test_sample_point_0060_TWAs_gas2(self):
        # test TWA calculations for nitrogen dioxide
        self._check_twas_for_one_firefighter_one_gas('0007', '2000-01-01 12:05:00', NITROGEN_DIOXIDE_COL,
                                                     expected_values=[0.02, 0.02, 0.01, 0.0, 0.0])

    def test_sample_point_0060_Gauges_gas1(self):
        # test Gauge calculations for carbon monoxide
        self._check_gauges_for_one_firefighter_one_gas('0007', '2000-01-01 12:05:00', CARBON_MONOXIDE_COL,
                                                     expected_values=[0, 0, 0, 0, 0], expected_status=GREEN)
    def test_sample_point_0060_Gauges_gas2(self):
        # test Gauge calculations for nitrogen dioxide
        self._check_gauges_for_one_firefighter_one_gas('0007', '2000-01-01 12:05:00', NITROGEN_DIOXIDE_COL,
                                                     expected_values=[0, 2, 1, 0, 0], expected_status=GREEN)


    def test_sample_point_0070_TWAs_gas1(self):
        # test TWA calculations for carbon monoxide
        self._check_twas_for_one_firefighter_one_gas('0007', '2000-01-01 13:00:00', CARBON_MONOXIDE_COL,
                                                     expected_values=[11.8, 13.6, 10.9, 2.7, 1.4])
    def test_sample_point_0070_TWAs_gas2(self):
        # test TWA calculations for nitrogen dioxide
        self._check_twas_for_one_firefighter_one_gas('0007', '2000-01-01 13:00:00', NITROGEN_DIOXIDE_COL,
                                                     expected_values=[0.59, 0.68, 0.55, 0.14, 0.07])

    def test_sample_point_0070_Gauges_gas1(self):
        # test Gauge calculations for carbon monoxide
        self._check_gauges_for_one_firefighter_one_gas('0007', '2000-01-01 13:00:00', CARBON_MONOXIDE_COL,
                                                     expected_values=[3.0, 9.0, 13.0, 8.0, 5.0], expected_status=GREEN)
    def test_sample_point_0070_Gauges_gas2(self):
        # test Gauge calculations for nitrogen dioxide
        self._check_gauges_for_one_firefighter_one_gas('0007', '2000-01-01 13:00:00', NITROGEN_DIOXIDE_COL,
                                                     expected_values=[12.0, 68.0, 55.0, 28.0, 14.0], expected_status=GREEN)


    def test_sample_point_0080_TWAs_gas1(self):
        # test TWA calculations for carbon monoxide
        self._check_twas_for_one_firefighter_one_gas('0008', '2000-01-01 12:40:00', CARBON_MONOXIDE_COL,
                                                     expected_values=[32.8, 19.0, 16.5, 7.8, 3.9])
    def test_sample_point_0080_TWAs_gas2(self):
        # test TWA calculations for nitrogen dioxide
        self._check_twas_for_one_firefighter_one_gas('0008', '2000-01-01 12:40:00', NITROGEN_DIOXIDE_COL,
                                                     expected_values=[1.64, 0.95, 0.82, 0.39, 0.19])

    def test_sample_point_0080_Gauges_gas1(self):
        # test Gauge calculations for carbon monoxide
        self._check_gauges_for_one_firefighter_one_gas('0008', '2000-01-01 12:40:00', CARBON_MONOXIDE_COL,
                                                     expected_values=[8.0, 13.0, 20.0, 24.0, 14.0]) # note: will be 'YELLOW' due to NO2
    def test_sample_point_0080_Gauges_gas2(self):
        # test Gauge calculations for nitrogen dioxide
        self._check_gauges_for_one_firefighter_one_gas('0008', '2000-01-01 12:40:00', NITROGEN_DIOXIDE_COL,
                                                     expected_values=[33.0, 95.0, 82.0, 78.0, 38.0], expected_status=YELLOW)


    def test_sample_point_0090_TWAs_gas1(self):
        # test TWA calculations for carbon monoxide
        self._check_twas_for_one_firefighter_one_gas('0008', '2000-01-01 13:30:00', CARBON_MONOXIDE_COL,
                                                     expected_values=[20.9, 13.4, 15.2, 10.2, 5.1])
    def test_sample_point_0090_TWAs_gas2(self):
        # test TWA calculations for nitrogen dioxide
        self._check_twas_for_one_firefighter_one_gas('0008', '2000-01-01 13:30:00', NITROGEN_DIOXIDE_COL,
                                                     expected_values=[1.04, 0.67, 0.76, 0.51, 0.25])

    def test_sample_point_0090_Gauges_gas1(self):
        # test Gauge calculations for carbon monoxide
        self._check_gauges_for_one_firefighter_one_gas('0008', '2000-01-01 13:30:00', CARBON_MONOXIDE_COL,
                                                     expected_values=[5, 9, 18, 31, 19]) # note: will be 'RED' due to NO2
    def test_sample_point_0090_Gauges_gas2(self):
        # test Gauge calculations for nitrogen dioxide
        self._check_gauges_for_one_firefighter_one_gas('0008', '2000-01-01 13:30:00', NITROGEN_DIOXIDE_COL,
                                                     expected_values=[21.0, 67.0, 76.0, 102.0, 50.0], expected_status=RED)



if __name__ == '__main__':
    unittest.main()
