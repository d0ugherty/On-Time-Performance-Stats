# Functions that can be called to hopefully make my life easier
# and make the project more readable

class helper():

    # return list of train ids with the longest delays
    def get_train_ids(df,lines):
        lst = []
        for line in lines:
            temp_df = df.loc[(df['line'] == line)]
            lst.append((temp_df.loc[temp_df['delay_minutes'] == temp_df['delay_minutes'].max(), 'train_id']))
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
            lst.append(float(temp_df['delay_minutes'].mean()))
        return lst
    