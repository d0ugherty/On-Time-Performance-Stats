# Functions that can be called to hopefully make my life easier
# and make the project more readable

import scipy.stats as stats
import pandas as pd
import numpy as np
import statistics
import matplotlib.pyplot as plt
class helper():

    # return list of train ids with the longest delays
    def get_train_ids(df,lines):
        lst = []
        for line in lines:
            temp_df = df.loc[(df['line'] == line)]
            lst.append(temp_df[temp_df['delay_minutes'] == temp_df['delay_minutes'].max()]['train_id'])
        return lst
    
    # return list of the longest delay (in minutes) for each line
    def get_max_delay(df,lines):
        test_list = []
        for line in lines:
            temp_df = df.loc[(df['line'] == line)]
            test_list.append(float(temp_df['delay_minutes'].max()))
        return test_list
    
    # return list of the average delay (in minutes) for each line
    def get_avg_delay_by_line(df,lines):
        lst = []
        for line in lines:
            temp_df = df.loc[(df['line'] == line)]
            temp_df = temp_df.astype({'delay_minutes' : 'float64'})
            lst.append(float(temp_df['delay_minutes'].mean()))
        return lst

    # return list of dates which had the longest delays
    def get_delay_date(df,lines):
        lst = []
        for line in lines:
            try:
                temp_df = df.loc[(df['line'] == line)]
                temp_df = temp_df.astype({'date':'datetime64[ns]'})
                date = temp_df[temp_df['delay_minutes'] == temp_df['delay_minutes'].max()]['date']
                lst.append(date.values[0])
            except:
                print("get delay date error")
        return lst
    
    ### create new dataframe and assign types
    ### this is used for breaking down performence by rail line
    def get_avg_longest(list_njt_lines, list_max_delays, list_avg_delays, list_dates):
        df = pd.DataFrame({'Longest Delay (minutes)'    : list_max_delays.values,
                            'Average Delay (minutes)'   : list_avg_delays.values,
                            'Date of Longest Delay'     : list_dates.values},
                            index=list_njt_lines)
        df = df.astype({'Longest Delay (minutes)'   :'float16',
                        'Average Delay (minutes)'   :'float16',
                        'Date of Longest Delay'     : 'datetime64[ns]'
                            })
        return df
    
    ### iterates through the data frame to retrieve the values of the delay_minutes column
    ### in each row. 
    ### count[0] = on time
    ### count[1] = 6-10 mins late
    ### count[2] = 10-15 mins late
    ### count[3] = >15 mins late
    def count_lateness(dataframe):
        count = [0, 0, 0, 0]
        column = dataframe['delay_minutes']
        for time in column:
            if(time < 6.0):
                count[0] += 1
            elif((time >= 6.0) & (time < 10.0)):
                count[1] += 1
            elif((time >= 10.0) & (time < 15.0)):
                count[2] += 1
            else:
                count[3] += 1
        return count
    
    ### Categorizes lateness by taking the 'delay_minutes' value
    ### of a row and entering it a correspondig array.
    ### Returns a combined array.
    def categroize_lateness(dataframe):
        lt_6 = []
        six_to_ten = []
        ten_to_fifteen = []
        gt_15 = []
        column = dataframe['delay_minutes']
        for time in column:
            if(time < 6.0):
                lt_6.append(time)
            elif((time >= 6.0) & (time < 10.0)):
                six_to_ten.append(time)
            elif((time >= 10) & (time < 15.0)):
                ten_to_fifteen.append(time)
            else:
                gt_15.append(time)
        all_arrays = np.array([lt_6, six_to_ten, ten_to_fifteen, gt_15], dtype='object')
        return all_arrays
    ########################################
    
    def get_otp_data(dataframe,column_name, categories, year = None, month = None):
        otp_data = []
        if((year == None) & (month == None)):
                print("none")
                for item in categories:
                        otp_item = helper.count_lateness(dataframe[(dataframe[column_name] == item)])
                        otp_data.append(otp_item)
                return otp_data
        elif((year != None) & (month != None)):
                print("year and month")
                for item in categories:
                        otp_item = helper.count_lateness((dataframe[column_name] == item) & 
                                                              (dataframe['date'].dt.year == year) & 
                                                              (dataframe['date'].dt.month == month))
                        otp_data.append(otp_item)
                return otp_data
        elif (year != None):
                print("by year")
                for item in categories:
                        otp_item = helper.count_lateness(dataframe[(dataframe[column_name] == item) & 
                                                                           (dataframe['date'].dt.year == year)])
                        otp_data.append(otp_item)
                return otp_data
        elif (month != None):
             print("by month")
             for item in categories:
                        otp_item = helper.count_lateness(dataframe[(dataframe[column_name] == item) & 
                                                                           (dataframe['date'].dt.month == month)])
                        otp_data.append(otp_item)
        return np.asarray(otp_data)
        
    ### calculate the on time performance of a dataframe
    ### OTP is calculated Services On Time divided by Total Services multipleid by 100
    def on_time_performance(dataframe):
        count = helper.count_lateness(dataframe)
        on_time = count[0]
        total_srvc = on_time + count[1] + count[2] + count[3]
        if total_srvc == 0:
            return None
        else:
            return round((on_time / total_srvc) * 100, 2)
    
    ### calculate the probability of a train being late
    ### essentially the on_time_perforamnce() function upside down
    def late_prob(dataframe):
         count = helper.count_lateness(dataframe)
         late_trains = count[1] + count[2] + count[3]
         total_srvc = late_trains + count[0]
         if total_srvc == 0:
              return None
         else:
              return round((late_trains / total_srvc), 2)

    ### get the standard deviation
    def calculate_std_dev(on_time_percentages):
        std_dev = statistics.stdev(on_time_percentages)
        return std_dev

    
    ### builds dataframes of services termination at a given destination
    ### and returns the on time performance for each time interval
    def otp_for_destination(dataframe, destination):
        trains_to_dest = dataframe[(dataframe['to'] == destination)]
        #trains_from_nyp = all_services[(all_services['from'] == 'New York Penn Station')]
        am_start = pd.to_datetime("06:00:00")
        am_end = pd.to_datetime("09:30:00")
        pm_start = pd.to_datetime("16:00:00")
        pm_end = pd.to_datetime("19:00:00")
        # AM Peak
        am_peak_dest = trains_to_dest[(trains_to_dest['scheduled_time'].dt.time >= am_start.time()) & 
                                      (trains_to_dest['scheduled_time'].dt.time <= am_end.time())]
        am_peak_dest = am_peak_dest.drop(am_peak_dest[am_peak_dest['date'].dt.weekday > 4].index)

        # PM Peak
        pm_peak_dest = trains_to_dest[(trains_to_dest['scheduled_time'].dt.time >= pm_start.time()) & 
                                      (trains_to_dest['scheduled_time'].dt.time <= pm_end.time())]
        pm_peak_dest = pm_peak_dest.drop(pm_peak_dest[pm_peak_dest['date'].dt.weekday > 4].index)

        #Off Peak
        off_peak1 = trains_to_dest[(trains_to_dest['scheduled_time'].dt.time < am_start.time())]
        off_peak2 = trains_to_dest[(trains_to_dest['scheduled_time'].dt.time > am_end.time()) & 
                                   (trains_to_dest['scheduled_time'].dt.time < pm_start.time())]
        off_peak3 = trains_to_dest[(trains_to_dest['scheduled_time'].dt.time > pm_end.time())]
        off_peak_dest = pd.concat([off_peak1, off_peak2])
        off_peak_dest = pd.concat([off_peak3, off_peak_dest])
        off_peak_dest = off_peak_dest.drop(off_peak_dest[off_peak_dest['date'].dt.weekday > 4].index)

        #all weekday
        weekday_dest = trains_to_dest[(trains_to_dest['scheduled_time'].dt.weekday <= 4)]
        #all weekend
        weekend_dest = trains_to_dest[(trains_to_dest['scheduled_time'].dt.weekday > 4)]

        am_peak_otp =  0.0 if am_peak_dest.empty else helper.on_time_performance(am_peak_dest)
        pm_peak_otp = 0.0 if pm_peak_dest.empty else helper.on_time_performance(pm_peak_dest)
        off_peak_otp = 0.0 if off_peak_dest.empty else helper.on_time_performance(off_peak_dest)
        weekday_otp = 0.0 if weekday_dest.empty else helper.on_time_performance(weekday_dest)
        weekend_otp = 0.0 if weekend_dest.empty else helper.on_time_performance(weekend_dest)

        otps = [am_peak_otp, pm_peak_otp, off_peak_otp, weekday_otp, weekend_otp]

        return otps
    
    ##### Get monthly OTP
    def get_monthly_otps(df):
        monthly_otps = []
        for year in df['year'].unique():
            for month in df['month'].unique():
                monthly_services = df[(df['year']==year) & (df['month']==month)]
                monthly_otps.append(helper.on_time_performance(monthly_services))
        return monthly_otps
    
    ### Returns array of delay_minutes values for late arrivals
    def get_late_arrivals(df):
        late_arrivals =[]
        for delay in df['delay_minutes']:
             if(delay > 5.99):
                  late_arrivals.append(delay)
        return late_arrivals
    
    ### Prints proababilties using CDF
    def print_prob_cdf(df, destination):
        print("CUMULATIVE DISTRIBUTIONS FOR " + destination.upper())
        print('-----------------------------------------------------')
        trains_to_dest = df[(df['to'] == destination)].copy()
        # group delay_minutes based on value
        delay_minutes = trains_to_dest['delay_minutes']
        count_minutes = helper.count_lateness(trains_to_dest)
        print(count_minutes[3]/(count_minutes[0] + count_minutes[1] + count_minutes[2] + count_minutes[3]))
        delay_minutes = np.array(trains_to_dest['delay_minutes'])
        # Setup Cumulative Distribution Function
        mu = np.mean(delay_minutes)
        sigma = statistics.stdev(delay_minutes)
        print(f'Mu: {mu}    Sigma: {sigma}')
        print(f'P(Y < 6) = norm.cdf(6, loc={mu}, scale={sigma}) = {stats.norm.cdf(6, loc=mu, scale=sigma):.4f}')
        print(f'P(6 < Y < 10) = stats.norm.cdf(10, loc={mu}, scale={sigma}) - stats.norm.cdf(6, loc={mu}, scale={sigma}) = ')
        print(f'{stats.norm.cdf(10, loc=mu, scale=sigma) - stats.norm.cdf(6, loc=mu, scale=sigma):.4f}')
        print(f'P(10 < Y < 15) = stats.norm.cdf(15, loc={mu}, scale={sigma}) - stats.norm.cdf(10, loc={mu}, scale={sigma}) = ')
        print(f'{stats.norm.cdf(15, loc=mu, scale=sigma) - stats.norm.cdf(10, loc=mu, scale=sigma):.4f}')
        print(f'P(Y > 15) = 1 - norm.cdf(15, loc={mu}, scale={sigma}) = {1 - stats.norm.cdf(6, loc=mu, scale=sigma):.4f}\n')
