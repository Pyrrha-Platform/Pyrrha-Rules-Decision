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

# FIELD / COLUMN NAMES
FIREFIGHTER_ID_COL = 'firefighter_id'
TIMESTAMP_COL = 'timestamp_mins'
CARBON_MONOXIDE_COL = 'carbon_monoxide'
TWA_COL_SUFFIXES = ['_twa_10min', '_twa_30min', '_twa_60min', '_twa_4hr', '_twa_8hr']

# ---------------------------------------

logging.basicConfig(level=logging.WARNING) # set to logging.INFO if you want to see all the routine INFO messages

# Unit tests for the GasExposureAnalytics class.
class GasExposureAnalyticsTestCase(unittest.TestCase):

    # Load a known sensor log, so that we can calculate gas exposure analytics for
    #  it and test that they match expectations.
    def setUp(self):
        self._analytics_test = GasExposureAnalytics(TEST_DATA_CSV_FILEPATH)

    def tearDown(self):
        pass

    # Core utility method for testing :
    # * Takes in a mocked 'now' timestamp, a firefighter and a gas
    # * Calculates the time-weighted average (TWA) exposure of that firefighter to that gas, at that time
    #   during the burn test dataset.
    # * Checks that the calculated results for all TWA windows match the given 'expected' values.
    # * If there are any discrepancies, generates a detailed description for debugging.
    def _check_twas_for_one_firefighter_one_gas(self, firefighter, timestamp_str, gas, expected_values):

        test_key = (firefighter, pd.Timestamp(timestamp_str))

        # Get the field names for the various expected analytic results
        gas_twas = [gas+twa for twa in TWA_COL_SUFFIXES]

        # Calculate time-weighted average (TWA) exposure of the given firefighter to the given gas, at the given time
        # during the burn test dataset.
        result_df = self._analytics_test.run_analytics(pd.Timestamp(timestamp_str) + pd.Timedelta(minutes=1), commit=False)

        # Check that the calculated results for all TWA windows match the given 'expected' values.
        # 1. Check that there *is* a result.
        self.assertTrue((result_df is not None),
                        "No analytics results produced for any firefighter at '"+timestamp_str+"')")
        try :
            # 2. Reformat the result a bit, to make the next steps simpler.
            # Context: The TWA time-windows (10m, 30m, etc.) start to fall out of scope towards the end of an
            # event - e.g. there's no '10 min TWA' being produced 20 mins after an event ends, but there *are* still
            # 30min, 1hr, etc TWAs being produced.  So the reindex() here just inserts null values for any missing
            # TWAs - which allows subsequent code not to have to worry about the 'shape' of the results.
            actual_value = (result_df.reindex(columns=gas_twas)
                .loc[test_key, gas_twas])
        except KeyError :
            # If the results contain TWAs for other firefighters, but not the expected firefighter.
            self.assertTrue(False, "No analytics results available for ('"+firefighter+"', '"+timestamp_str+"')")

        rounded_actual_values = list(np.round(actual_value,1))

        # Check that the calculated results for all TWA windows match the given 'expected' values.
        test_failed = False
        for (gas_twa, expected_value, rounded_actual_value) in zip(gas_twas, expected_values, rounded_actual_values) :
            # Check if both the expected and actual are NaNs, as we can't check (NaN == NaN) - not valid in python
            both_NaN = pd.isna([expected_value, rounded_actual_value]).all() 
            self.assertTrue((expected_value == rounded_actual_value) or (both_NaN),
                            (("%s expected to be %s but was %s "
                              +"\n\t(debug: %s - firefighter '%s' - expected: %s - actuals: %s)")
                             % (gas_twa, expected_value, rounded_actual_value, timestamp_str, firefighter,
                                expected_values, rounded_actual_values)))


    # Utility method for inspecting specifc firefighter metrics when writing unit tests, or debugging tests that fail.
    # Requires an analytic dataframe that contains all the computed analytics for a full burn event.
    def inspect_twas_for_one_gas_for_one_firefighter(df, firefighter, timestamp_str, gas, pad_mins=10) :
        gas_twas = [gas+twa for twa in twas]
        timestamp = pd.Timestamp(timestamp_str)
        padding = pd.Timedelta(minutes=pad_mins)
        range_df = df.reset_index().set_index(TIMESTAMP_COL).loc[timestamp - padding : timestamp + padding, :]
        range_df = (range_df.loc[range_df[FIREFIGHTER_ID_COL] == firefighter,
                    [FIREFIGHTER_ID_COL] + [gas+twa for twa in TWA_COL_SUFFIXES]
                    ])
        display(range_df)        


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

    def test_first_sensor_record_of_the_day(self):
        # The very first record of the day - 1 reading for 1 firefighter!
        self._check_twas_for_one_firefighter_one_gas('0007', '2000-01-01 09:32:00', CARBON_MONOXIDE_COL,
                                                     expected_values=[0.2, 0.1, 0.0, 0.0, 0.0])


    def test_missing_some_sensor_readings_in_10min_window(self):
        # 10 min window for firefighter 0003 is only half full.
        self._check_twas_for_one_firefighter_one_gas('0003', '2000-01-01 15:03:00', CARBON_MONOXIDE_COL,
                                                     expected_values=[1.8, 4.8, 6.0, 9.4, 6.0])
        # TODO: when we add dropout processing, this might need to change (e.g. server might 'decline' to
        # calculate some of the other TWAs (e.g. 30m) if there aren't enough records)


    def test_no_sensor_readings_in_10min_window(self):
        # 10 min window for firefighter 0003 is now empty.
        self._check_twas_for_one_firefighter_one_gas('0003', '2000-01-01 15:08:00', CARBON_MONOXIDE_COL,
                                                     expected_values=[np.nan, 3.7, 5.3, 9.2, 6.0])
        # TODO: when we add dropout processing, this might need to change (e.g. server might 'decline' to
        # calculate some of the other TWAs (e.g. 30m) if there aren't enough records)


    def test_no_sensor_readings_in_30min_window(self):
        # 30 min window for firefighter 0003 is now empty.
        self._check_twas_for_one_firefighter_one_gas('0003', '2000-01-01 15:28:00', CARBON_MONOXIDE_COL,
                                                     expected_values=[np.nan, np.nan, 3.0, 7.9, 6.0])
        # TODO: when we add dropout processing, this might need to change (e.g. server might 'decline' to
        # calculate some of the other TWAs (e.g. 60m) if there aren't enough records)


    def test_no_sensor_readings_in_30min_window_alt(self):
        # ...just checking for another FF (so it's not just '0003')
        # As we head out past 30 mins after the last sensor reading, the 10m and 30m TWAs drop off
        self._check_twas_for_one_firefighter_one_gas('0010', '2000-01-01 14:00:00', CARBON_MONOXIDE_COL,
                                                     expected_values=[np.nan, np.nan, 3.1, 7.3, 3.6])


    def test_no_sensor_readings_in_60min_window(self):
        # 60 min window for firefighter 0003 is now empty.
        self._check_twas_for_one_firefighter_one_gas('0003', '2000-01-01 15:58:00', CARBON_MONOXIDE_COL,
                                                     expected_values=[np.nan, np.nan, np.nan, 6.0, 6.0])


    def test_no_sensor_readings_in_4hr_window(self):
        # 4 hr window for firefighter 0003 is now empty.
        self._check_twas_for_one_firefighter_one_gas('0003', '2000-01-01 18:58:00', CARBON_MONOXIDE_COL,
                                                     expected_values=[np.nan, np.nan, np.nan, np.nan, 4.7])


    def test_second_last_sensor_record_of_the_day(self):
        # The second-last TWA calculation of the day for '0008'
        self._check_twas_for_one_firefighter_one_gas('0008', '2000-01-01 22:48:00', CARBON_MONOXIDE_COL,
                                                     expected_values=[np.nan, np.nan, np.nan, np.nan, 0])


    def test_last_sensor_record_of_the_day(self):
        # The last TWA calculation of the day for '0008'
        self._check_twas_for_one_firefighter_one_gas('0008', '2000-01-01 22:49:00', CARBON_MONOXIDE_COL,
                                                     expected_values=[np.nan, np.nan, np.nan, np.nan, 0])


    def test_correct_twas_still_provided_when_live_sensor_is_offline(self):
        # This test covers a period when there are sensor dropouts, but the TWAs are OK - to ensure
        # the analytics work correctly regardless of dropouts
        self._check_twas_for_one_firefighter_one_gas('0006', '2000-01-01 12:05:00', CARBON_MONOXIDE_COL,
                                                     expected_values=[34.6, 82.0, 64.8, 21.2, 10.6]) # (CO dropout)


    def test_correct_twas_still_provided_when_live_sensor_is_offline_alt(self):
        # This test covers a period when there are sensor dropouts, but the TWAs are OK - to ensure
        # the analytics work correctly regardless of dropouts
        # ...just checking for another FF (so it's not just '0006')
        self._check_twas_for_one_firefighter_one_gas('0010', '2000-01-01 12:05:00', CARBON_MONOXIDE_COL,
                                                     expected_values=[10.3, 10.8, 13.6, 4.5, 2.3]) # (CO dropout)


    def test_after_the_last_twa_of_the_day(self):
        # No more TWA data - everything run past the end of the 8hr longest window - assert empty
        post_end_test_df = self._analytics_test.run_analytics(pd.Timestamp('2000-01-01 22:58:00'), commit=False)
        # Doesn't return a dataframe (or shouldn't)
        assert post_end_test_df is None, "Expected to find no sensor records more than 8hrs after the event, but found "\
                                         + str(post_end_test_df.index.size)


    # #################################################################################
    #  GENERAL SAMPLING TESTS
    # #################################################################################


    def test_sample_point_0010(self):
        self._check_twas_for_one_firefighter_one_gas('0001', '2000-01-01 11:24:00', CARBON_MONOXIDE_COL,
                                                     expected_values=[8.6, 3.0, 2.1, 0.7, 0.3])

    def test_sample_point_0020(self):
        self._check_twas_for_one_firefighter_one_gas('0010', '2000-01-01 11:30:00', CARBON_MONOXIDE_COL,
                                                     expected_values=[20.5, 13.8, 8.9, 2.7, 1.4])

    def test_sample_point_0030(self):
        self._check_twas_for_one_firefighter_one_gas('0003', '2000-01-01 11:15:00', CARBON_MONOXIDE_COL,
                                                     expected_values=[6.1, 6.0, 6.2, 2.9, 1.4])


    def test_sample_point_0040(self):
        self._check_twas_for_one_firefighter_one_gas('0003', '2000-01-01 11:35:00', CARBON_MONOXIDE_COL,
                                                     expected_values=[29.3, 16.0, 11.2, 4.6, 2.3])


    def test_sample_point_0050(self):
        self._check_twas_for_one_firefighter_one_gas('0006', '2000-01-01 10:35:00', CARBON_MONOXIDE_COL,
                                                     expected_values=[44.9, 25.0, 13.1, 3.3, 1.6])


    def test_sample_point_0060(self):
        self._check_twas_for_one_firefighter_one_gas('0007', '2000-01-01 12:05:00', CARBON_MONOXIDE_COL,
                                                     expected_values=[0.4, 0.4, 0.2, 0.1, 0.0])


    def test_sample_point_0070(self):
        self._check_twas_for_one_firefighter_one_gas('0007', '2000-01-01 13:00:00', CARBON_MONOXIDE_COL,
                                                     expected_values=[11.8, 13.4, 11.1, 2.8, 1.4])


    def test_sample_point_0080(self):
        self._check_twas_for_one_firefighter_one_gas('0008', '2000-01-01 12:40:00', CARBON_MONOXIDE_COL,
                                                     expected_values=[29.9, 18.8, 16.2, 7.1, 3.5])


    def test_sample_point_0090(self):
        self._check_twas_for_one_firefighter_one_gas('0008', '2000-01-01 13:30:00', CARBON_MONOXIDE_COL,
                                                     expected_values=[19.9, 13.2, 14.6, 9.5, 4.7])


    # #################################################################################
    # ### TODO: Tests that aren't working and need attention
    # #################################################################################

    # def test_last_twa_of_the_day(self):
    #     # 'last TWA calculation of the day' issue
    #     # Just before the 8hr window trails off for the very last firefighter (0003), we're getting this error :
    #     #.   KeyError: "None of [Index(['carbon_monoxide', 'nitrogen_dioxide', 'firefighter_id'], dtype='object')] are in the [columns]"
    #     # Other firefighter's 8hr window endings work OK, it's just this last one with the error
    #     # (See: "The second-last and last TWA calculation of the day for '0008'" above)
    #     # Also - it works as expected 1 min before and 1 min after.  So it's possibly something like a 1-minute
    #     # difference between the size of DB and pandas '8hr' framing (e.g. one is inclusive and the other exclusive).
    #     # All other records work as expected, so this test is disabled for now.
    #     self._check_twas_for_one_firefighter_one_gas('0003', '2000-01-01 22:56:00', CARBON_MONOXIDE_COL,
    #                                                  expected_values=[np.nan, np.nan, np.nan, np.nan, 4.7])





if __name__ == '__main__':
    unittest.main()
