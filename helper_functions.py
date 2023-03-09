# Functions that can be called to hopefully make my life easier
# and make the project more readable

import os
import pandas as pd
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
        new_df = current_df.astype({'STATION':'category',
                                    'DATE':'datetime64[ns]'})
        
        new_df.drop(labels=['ELEVATION', 'MDSF', 'AWND', 'SNWD',
                            'WESD', 'WESF','WT01', 'WT02', 'WT03',
                            'WT04', 'WT05',"WT06","WT07","WT08",
                            "WT09","WT11"], axis=1, inplace=True)
        if month == "" :
            return new_df
        else:
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
                        'scheduled_time' : 'datetime64',
                        'actual_time' : 'datetime64',
                        'delay_minutes' : 'float16',
                        'line' : 'category',
                        'type' : 'category'},
                        errors='ignore')
        new_df.drop(new_df[new_df['line'] == 'Meadowlands Rail'].index, inplace=True)
        print("Done formatting dataframe")
        return new_df

    ### create new dataframe and assign types
    ### this is used for breaking down performence by rail line
    def create_new_dataframe(list_njt_lines, list_max_delays, list_avg_delays, list_dates):
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
    ### count[1] = 3-5 mins late
    ### count[2] = 5-10 mins late
    ### count[3] = >10 mins late
    def categorize_lateness(dataframe):
        count = [0, 0, 0, 0]
        column = dataframe['delay_minutes']
        for delay in column:
            if(delay < 3.0):
                count[0] += 1
            elif((delay >= 3.0) & (delay < 5.0) ):
                count[1] += 1
            elif((delay >= 5.0) & (delay <= 10.0)):
                count[2] += 1
            else:
                count[3] += 1
        return count
