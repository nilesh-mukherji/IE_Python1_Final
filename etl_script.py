# %% [markdown]
# ## Python 1 Project
# #### IE MBD Apr 2024
# #### Group 4

# %%
import pandas as pd
import sqlalchemy as sql
import matplotlib
from ydata_profiling import ProfileReport
import argparse

# %%
def read(path: str = None, db_config: dict = None):
    """
    This function should read a filepath/DB and return a dataframe. It checks against default Nones,
    and will use the appropriate read function based on source. 
    """

    if db_config != None and db_config != None:
        raise TypeError("What should we read?")
    elif db_config == None:
        return pd.read_csv(path)
    elif path == None:
        return pd.read_sql_table("COVID_DATA", db_config)
    else:
        raise TypeError("What should we read???")

# %%
def processFiles(paths: dict = None, dbConfig: dict= None) -> tuple:
    """ 
    Read files from the relevant paths and use functions above to clean. 
    This does a bit of early filtering by dropping columns which are entirely empty. 
    """

    retVal = []
    if paths != None:
        for path in paths['lst']:
            raw_df = read(paths['path_root']+path)
            raw_df = raw_df.dropna(axis=1, how="all").drop_duplicates()
            retVal.append(raw_df)
    if dbConfig != None:
        for path in dbConfig:
            raw_df = read(paths['path_root']+path)
            raw_df = raw_df.dropna(axis=1, how="all").drop_duplicates()
            retVal.append(raw_df)
    
    return retVal



# %%
def joinFrames(df_list: list, start_date, end_date, countries) -> pd.DataFrame:
    """
    Join DF's together
    """

    def normalize_dates(df1, col):
        date_min = pd.to_datetime(df1.groupby('country_name').date.min().min())
        date_max = pd.to_datetime(df1.groupby('country_name').date.max().max())


        def create_row(df1, col, country, date, format):
                tmp = {col: country, 'date': date}
                for x in df1.columns:
                    if x not in ['date', col]:
                        tmp[x] = format[x].array[0]
                return tmp
        
        for country in df1[col].unique():
            cols = []
            max = df1[df1[col]==country].date.max()
            min = df1[df1[col]==country].date.min()

            min_col = df1[(df1[col] == country) & (df1['date'] == min)]
            date = pd.to_datetime(min) - pd.Timedelta(1, "D")

            # print(country, "min", min_col)

            while date >= date_min:
                cols.append(create_row(df1, col, country, date, min_col))
                date = date - pd.Timedelta(1, "D")

            date = pd.to_datetime(max) + pd.Timedelta(1, "D")
            max_col = df1[(df1[col] == country) & (df1['date'] == max)]
            while date <= date_max:
                cols.append(create_row(df1, col, country, date, max_col))
                date = date + pd.Timedelta(1, "D")

            # print(country, "max", max_col)
            df1 = pd.concat([df1, pd.DataFrame(cols)])
        return df1


    retVal = 1
    for df in df_list:
        if isinstance(retVal, int):
            retVal = df
        else:
            if 'date' in df.columns:
                if 'date' in retVal.columns:
                    retVal = retVal.merge(df, on=["location_key", "date"], how="outer")
                    continue

            retVal = retVal.merge(df, on="location_key", how="outer")
    
    retVal = retVal.drop(['place_id', 'wikidata_id',
       'datacommons_id', 'country_code', 'subregion1_code',
       'subregion1_name', 'subregion2_code', 'subregion2_name',
       'locality_code', 'locality_name', 'iso_3166_1_alpha_2',
       'iso_3166_1_alpha_3', 'aggregation_level', "location_key", "new_confirmed", "new_deceased"], axis=1)

    retVal['cumulative_persons_fully_vaccinated'] = retVal['cumulative_persons_fully_vaccinated'].fillna(0)

    #Null Thresholds are the data population threshold for dropping Null Values within columns. 
    null_thresh = 0.7
    retVal.dropna(axis=1, thresh=len(retVal)*null_thresh, inplace=True)

    #Handling any aditional rows with Null Values
    retVal.dropna(axis=0, inplace=True)
    
    retVal['date'] = pd.to_datetime(retVal['date'])

    retVal = retVal[(retVal['date'] > start_date) & (retVal['date'] < end_date)]
    if countries != None: retVal = retVal[retVal['country_name'].str.upper().isin(countries)]
    
    true_frame = retVal.groupby(["country_name", pd.Grouper(key='date', freq='W-MON')]).last()
    true_frame = true_frame.reset_index()
    true_frame['date'] = pd.to_datetime(true_frame['date'])
    true_frame = normalize_dates(true_frame, 'country_name')
    true_frame = true_frame.groupby(["country_name", pd.Grouper(key='date', freq='W-MON')]).last()

    return true_frame

# %%
def write(df: pd.DataFrame, dbConfig: dict = None, path: str = None):
    """
    Write to CSV/DB
    """
    if not dbConfig and not path:
        raise KeyError("Nowhere to Write")
    elif not dbConfig:
        df.to_csv(path)
        return 1
    else:
        df.to_sql("COVID_DATA", dbConfig)
        return


# %%
def plot(df: pd.DataFrame) -> None:
    """
    Plot relevant data from the DF
    """

# %%
def createData(inpath, outpath, start_date, end_date, countries) -> None:
    if countries: countries = [i.strip() for i in countries[0].upper().split(',')]
    start_date = pd.to_datetime(start_date)
    end_date = pd.to_datetime(end_date)

    # DB Config
    #engine = sql.create_engine("test+testdb://nilesh:password@localhost/test")

    files_config = {
        'lst': ["demographics", "epidemiology", "health", "hospitalizations", "index", "vaccinations"],
        'path_root': inpath
    }
    clean_dfs = processFiles(paths=files_config)
    merged_dfs = joinFrames(clean_dfs, start_date, end_date, countries)
    write(merged_dfs, path=outpath+"macrotable.csv")

    return merged_dfs




if __name__ == "__main__":
    parser = argparse.ArgumentParser(description='Run ETL')
    parser.add_argument('dir', type=str, help="Directory to data.")
    
    parser.add_argument('-o', type=str, nargs=1, default='./', required=False)

    parser.add_argument('-start', type=str, nargs=1, default='2020-01-02', 
                    help='Start date', required=False)

    parser.add_argument('-end', metavar='stop', type=str, nargs=1, default='2022-08-22', 
                    help='End date', required=False)

    parser.add_argument('-countries', metavar='countries', type=str, nargs=1, default=None, 
                    help='Countries to analyze', required=False)

    res = parser.parse_args()
    df = createData(res.dir, res.o, res.start, res.end, res.countries)


