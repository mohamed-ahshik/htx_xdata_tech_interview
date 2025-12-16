def remove_duplicates(df):
    no_duplicates_df = df.drop_duplicates(keep='first', inplace='false')