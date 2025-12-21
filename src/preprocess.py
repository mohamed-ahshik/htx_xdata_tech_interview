import re
import pandas as pd 


def remove_duplicates(df:pd.DataFrame)->pd.DataFrame:
    """
    Remove duplicate rows from a pandas DataFrame.

    Parameters
    ----------
    df : pandas.DataFrame
        Input DataFrame from which duplicate rows will be removed.

    Returns
    -------
    pandas.DataFrame
        A new DataFrame with duplicate rows removed, keeping the first
        occurrence of each duplicate.
    """
    print(f"Duplicates :{df.duplicated().sum()}")
    no_duplicates_df = df.drop_duplicates(keep='first', inplace=False)
    print(f"After dropping Duplicates :{no_duplicates_df.duplicated().sum()}")
    return no_duplicates_df

def standarise_resale_price_datatype(df:pd.DataFrame)->pd.DataFrame:
    columns = df.columns
    column_name = 'resale_price'

    if column_name in columns:
        print(f"Column : {column_name} datatype before converting : {df[column_name].dtypes}")
        df = df.astype({column_name:'int'})
        print(f"Column : {column_name} datatype after converting : {df[column_name].dtypes}")  
    return df

def standarise_floor_area_sqm_datatype(df:pd.DataFrame)->pd.DataFrame:
    columns = df.columns
    column_name = 'floor_area_sqm'

    if column_name in columns:
        print(f"Column : {column_name} datatype before converting : {df[column_name].dtypes}")
        df = df.astype({column_name:'int'})
        print(f"Column : {column_name} datatype after converting : {df[column_name].dtypes}")  
    return df

def split_year_and_month(df:pd.DataFrame)->pd.DataFrame:
    df['month'] = pd.to_datetime(df['month'])
    df['month_only'] = df['month'].dt.month
    df['year_only'] = df['month'].dt.year
    
    return df

def create_remaining_lease_column(df:pd.DataFrame)->pd.DataFrame:
    columns = df.columns
    column_name = 'remaining_lease'
    
    if column_name not in columns:
        number_of_years_active = df['year_only'] - df['lease_commence_date']
        df['remaining_lease'] = 99 - number_of_years_active
        print('remaining_lease Column added')
    
    return df

def create_floor_area_sqft_column(df:pd.DataFrame)->pd.DataFrame:
    
    df["floor_area_sqft"] = (df["floor_area_sqm"] * 10.7639).astype(int)

    return df

def standarise_remaining_lease_datatype(df:pd.DataFrame)->pd.DataFrame:
    columns = df.columns
    column_name = 'remaining_lease'

    if column_name in columns:
        print(f"Column : {column_name} datatype before converting : {df[column_name].dtypes}")
        df[column_name] = df[column_name].apply(
            lambda x: re.search(r"(\d+)\s*years?", x).group(1)
            if isinstance(x, str) and "year" in x.lower()
            else x)

        df = df.astype({column_name:'int'})
        print(f"Column : {column_name} datatype after converting : {df[column_name].dtypes}")  
    return df

def split_storey_range(df:pd.DataFrame)->pd.DataFrame:
    df["storey_min"] = df["storey_range"].str.split(" ").str[0].astype(int)
    df["storey_max"] = df["storey_range"].str.split(" ").str[2].astype(int)
    
    return df

def create_psf_column(df:pd.DataFrame)->pd.DataFrame:
    
    df["psf_temp"] = (df["resale_price"] / df["floor_area_sqft"] * 100).round(2)
    df["psf"] = df["psf_temp"].astype(int) / 100
    
    return df

def preprocess_pipeline(df:pd.DataFrame)->pd.DataFrame:
    df = remove_duplicates(df)
    df = standarise_resale_price_datatype(df)
    df = standarise_floor_area_sqm_datatype(df)
    df = split_year_and_month(df)
    df = create_remaining_lease_column(df)
    df = standarise_remaining_lease_datatype(df)
    df = create_floor_area_sqft_column(df)
    df = split_storey_range(df)
    df = create_psf_column(df)
    print('-----------------------------------')
    
    return df



if __name__ == "__main__":
    
    df = pd.read_csv('/Users/user/Documents/htx_xdata/htx_xdata_tech_interview/data/HDB/resale-flat-prices-based-on-approval-date-1990-1999.csv')
    print(preprocess_pipeline(df))
    