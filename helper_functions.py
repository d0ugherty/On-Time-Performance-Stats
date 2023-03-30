# Functions that can be called to hopefully make my life easier
# and make the project more readable

import os
import pandas as pd
import numpy as np
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
    def get_avg_delay(df,lines):
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

    ### Removes unused columns from weather dataframe and converts objects to datatypes. 
    ### Takes an optional month name string to return a dataframe for a single month  
    def format_weather(current_df, month=""):
        print("Formatting weather dataframe...")
        print("Changing datatypes...")
        new_df = current_df.astype({'STATION':'category',
                                    'DATE':'datetime64[ns]'})
        print("Dropping columns...")
        new_df.drop(labels=['ELEVATION','LATITUDE', 'LONGITUDE', 'MDSF', 'AWND', 'SNWD',
                            'WESD', 'WESF','WT01', 'WT02', 'WT03',
                            'WT04', 'WT05',"WT06","WT07","WT08",
                            "WT09","WT11"], axis=1, inplace=True)
        if month == "" :
            print("Done formatting weather dataframe")
            return new_df
        else:
            print("Done formatting weather dataframe")
            new_df_w_month = new_df.loc[new_df['DATE'].apply(pd.Timestamp.month_name) == month]
            return new_df_w_month
    
    ### clean up services dataframes
    ### Drop status column bc it's not used and lateness for Amtrak is not tracked
    ### in the CSV file. Meadowlands is a special service so that is dropped as well.
    def format_services(current_df):
        print("Dropping columns...")
        if 'status' in current_df:
            current_df.drop(labels=['status'], axis=1, inplace=True)
        current_df.drop(current_df[current_df['type'] == 'Amtrak'].index, inplace=True)
        print("Changing datatypes...")
        new_df = current_df.astype({'date' : 'datetime64[ns]',
                        'train_id' : 'category',
                        'stop_sequence' : 'float16',
                        'from' : 'category',
                        'from_id': 'category',
                        'to' : 'category',
                        'to_id': 'category',
                        'scheduled_time' : 'datetime64[ns]',
                        'actual_time' : 'datetime64[ns]',
                        'delay_minutes' : 'float16',
                        'line' : 'category',
                        'type' : 'category'},
                        errors='ignore')
        new_df.drop(new_df[new_df['line'] == 'Meadowlands Rail'].index, inplace=True)
        print("Done formatting dataframe")
        return new_df

    ### create new dataframe and assign types
    ### this is used for breaking down performence by rail line
    def get_avg_longest(list_njt_lines, list_max_delays, list_avg_delays, list_dates):
        df = pd.DataFrame({'Longest Delay (minutes)': list_max_delays.values,
                            'Average Delay (minutes)': list_avg_delays.values,
                            'Date of Longest Delay' : list_dates.values},
                            index=list_njt_lines)
        df = df.astype({'Longest Delay (minutes)' :'float16',
                        'Average Delay (minutes)' :'float16',
                        'Date of Longest Delay' : 'datetime64[ns]'
                            })
        return df
    
    ### combines all CSV files into a single dataframe
    ### it also exports the dataframe into a combined CSV file
    ### combines all CSV files into a single dataframe
    ### it also exports the dataframe into a combined CSV file
    def combine_csvs(directory):
        csv_files = [f for f in os.listdir(directory) if f.endswith('.csv')]
        dfs = []

        print("Combining CSV files....")
        for file in csv_files:
            df = pd.read_csv(os.path.join(directory, file))
            dfs.append(df)
        combined_df = pd.concat(dfs, ignore_index=True)
        print("Formatting new CSV file...")
        combined_df = helper.format_services(combined_df)
        # make a new CSV for the dataframe
        print("Exporting....")
        compression_opts = dict(method='zip', archive_name='df_output.csv')  
        combined_df.to_csv('out.zip', index=False, compression=compression_opts)

        print("CSV files successfully combined and exported.")  
        return combined_df

    ### iterates through the data frame to retrieve the values of the delay_minutes column
    ### in each row. 
    ### count[0] = on time
    ### count[1] = 6-10 mins late
    ### count[2] = 10-15 mins late
    ### count[3] = >15 mins late
    def categorize_lateness(dataframe):
        count = [0, 0, 0, 0]
        column = dataframe['delay_minutes']
        for delay in column:
            if(delay < 6.0):
                count[0] += 1
            elif((delay >= 6.0) & (delay < 10.0) ):
                count[1] += 1
            elif((delay >= 10.0) & (delay < 15.0)):
                count[2] += 1
            else:
                count[3] += 1
        return count
    
    ########################################
    
    def get_otp_data(dataframe,column_name, categories, year = None, month = None):
        otp_data = []
        if((year == None) & (month == None)):
                print("none")
                for item in categories:
                        otp_item = helper.categorize_lateness(dataframe[(dataframe[column_name] == item)])
                        otp_data.append(otp_item)
                return otp_data
        elif((year != None) & (month != None)):
                print("year and month")
                for item in categories:
                        otp_item = helper.categorize_lateness((dataframe[column_name] == item) & 
                                                              (dataframe['date'].dt.year == year) & 
                                                              (dataframe['date'].dt.month == month))
                        otp_data.append(otp_item)
                return otp_data
        elif (year != None):
                print("by year")
                for item in categories:
                        otp_item = helper.categorize_lateness(dataframe[(dataframe[column_name] == item) & 
                                                                           (dataframe['date'].dt.year == year)])
                        otp_data.append(otp_item)
                return otp_data
        elif (month != None):
             print("by month")
             for item in categories:
                        otp_item = helper.categorize_lateness(dataframe[(dataframe[column_name] == item) & 
                                                                           (dataframe['date'].dt.month == month)])
                        otp_data.append(otp_item)
        return np.asarray(otp_data)
    
    ### Takes an array of integer arrays as a parameter
    ### and iteratively plots the data
    def chart_subplots(data, cat_labels, leg_labels, otp_colors,title=""):
        fig = plt.figure(figsize=(15,12))
        for n, count in enumerate(data):
            ax = plt.subplot(3,4, n+1)
            ax.pie(data[n], colors=otp_colors, radius = 1.2, autopct = "%0.2f%%", startangle=270)
            ax.title.set_text(cat_labels[n])
        plt.legend(bbox_to_anchor = (1.5, 1.0), labels = leg_labels)
        fig.suptitle(title,fontsize=16)
        plt.show()
        
    ### calculate the on time performance of a dataframe
    ### OTP is calculated Services On Time divided by Total Services multipleid by 100
    def on_time_performance(dataframe):
        count = helper.categorize_lateness(dataframe)
        on_time = count[0]
        total_srvc = on_time + count[1] + count[2] + count[3]
        return round((on_time / total_srvc) * 100, 2)
    
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

        
