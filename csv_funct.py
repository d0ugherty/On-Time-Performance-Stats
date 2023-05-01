
import os
import pandas as pd
import numpy as np

class csv():

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
        if 'line' in current_df:
            current_df = current_df[current_df['line'] != 'Meadowlands Rail']
        if 'type' in current_df:
            current_df.drop(labels=['type'], axis = 1, inplace=True)
        if 'stop_sequence' in current_df:
            current_df.drop(labels=['stop_sequence'], axis=1, inplace=True)
        if ('from_id' in current_df) & ('to_id' in current_df):
            current_df.drop(labels=['from_id', 'to_id'], axis=1, inplace=True)
        print("Changing datatypes...")
        print(current_df)
        new_df = current_df.astype({'date' : 'datetime64[ns]',
                        'train_id' : 'category',
                        'from' : 'category',
                        'to' : 'category',
                        'scheduled_time' : 'datetime64[ns]',
                        'actual_time' : 'datetime64[ns]',
                        'delay_minutes' : 'float16',
                        'line' : 'category'},
                        errors='ignore')
        new_df.dropna(how='any', inplace=True)
        print("Done formatting dataframe")
        return new_df

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
        combined_df = csv.format_services(combined_df)
        # make a new CSV for the dataframe
        print("Exporting....")
        compression_opts = dict(method='zip', archive_name='all_services.csv')  
        combined_df.to_csv('out.zip', index=False, compression=compression_opts)

        print("CSV files successfully combined and exported.")  
        return combined_df