class scripts():

    def get_train_ids(df,lines):
        lst = []
        for line in lines:
            temp_df = df.loc[(df['line'] == line)]
            lst.append(temp_df.loc[df_])

    def get_max_delay(df,lines):
        test_list = []
        for line in lines:
            temp_df = df.loc[(df['line'] == line)]
            test_list.append(temp_df['delay_minutes'].max())
        return test_list
    
    def get_avg_delays(df,lines):
        lst = []
        for line in lines:
            temp_df = df.loc[(df['line'] == line)]
            lst.append(temp_df['delay_minutes'].mean())
        return ls
    