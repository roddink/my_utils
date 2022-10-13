import numpy as np
import pandas as pd
import re
from datetime import timedelta, datetime


def join_within_range(l_df, r_df, l_df_on, r_df_lrange, r_df_rrange, boundary='[]', how='inner'):
    """
    Join two dataframes with range conditions

    Params
    ------
    l_df: pd.DataFrame, the left dataframe, with column that is within the range
    r_df: pd.DataFrame, the right dataframe, with columns that defines the range
    l_df_on: str, the column name in l_df
    r_df_lrange: str, the left range column name in r_df
    r_df_rrange: str, the right range column name in r_df
    bounary: str, in '[]', '()', '(]', '[)', the round brackets mean the boundaries not included, 
            the square brackets mean the boundaries included.
    how: str, in 'inner', 'left', 'right', how to join the two dataframes.
    """
    # make the input immutable
    l_df = l_df.copy()
    r_df = r_df.copy()
    
    try:
        assert l_df_on in l_df.columns
        assert r_df_lrange in r_df.columns
        assert r_df_rrange in r_df.columns
        
    except AssertionError:
        raise ValueError('can\'t find condition columns in dataframes')

    # find common columns in left and right dataframe and add '_y' at the end of right dataframe
    common_columns = list(set(l_df.columns).intersection(r_df.columns))
    r_df = r_df.rename(columns=dict(zip(common_columns, 
                                        [col + '_y' for col in common_columns])))

    def _boundary(boundary,l_row):
        # form the join condition
        boundary_dict = {'[]': (r_df[r_df_rrange] >= l_row) & (r_df[r_df_lrange] <= l_row), 
                        '(]': (r_df[r_df_rrange] > l_row) & (r_df[r_df_lrange] <= l_row), 
                        '()': (r_df[r_df_rrange] > l_row) & (r_df[r_df_lrange] < l_row), 
                        '[)': (r_df[r_df_rrange] >= l_row) & (r_df[r_df_lrange] < l_row)}
        return boundary_dict[boundary]    
    
    def _assign_nan_to_columns(df, cols):
        # assign nans into dataframe the input columns
        for col in cols:
            df[col] = np.nan
        return df
    dfs = []
    idx_list = []

    # handle the inner and left join

    for idx, l_row in enumerate(l_df.iterrows()):
        df = r_df.loc[_boundary(boundary, l_row[1][l_df_on])]
        if how=='right':
            r_df.loc[_boundary(boundary, l_row[1][l_df_on]), '__count'] = 1 
        for col in l_row[1].index:
            # use assign to avoid warnings
            df = df.assign(**{col: l_row[1][col]})
        if df.shape[0] == 0:
            idx_list.append(idx)
        dfs.append(df)
    
    if how=='inner':
        return pd.concat(dfs)
    
    elif how=='left':
        l_df = _assign_nan_to_columns(l_df, r_df.columns)
        dfs.append(l_df.iloc[idx_list])
        return pd.concat(dfs)

    elif how=='right':
        r_df = _assign_nan_to_columns(r_df, l_df.columns)
        dfs.append(r_df.loc[r_df['__count'].isna()])
        return pd.concat(dfs).drop(columns='__count')


def pivot_events_to_snapshots(df, agg_func, start_time, end_time, timestamp, 
                              step, by=[], value_col='', 
                              timeformat='%Y-%m-%d %H:%M:%S', 
                              fill='ffill', default=0):
    """
    pivot the dataframe in event format to a snapshot format. 
    This is commonly used in sensor data process.

    parameters
    ----------
    df: pyspark.sql.DataFrame, input events list 
    agg_func: str or function, the aggregation function to apply to the data in each time step. 
            same with aggregation function in pandas groupby
    start_time: str or datetime.datetime, the starting time of the converted snapshot dataframe.
    end_time: str or datetime.datetime, the end time of the converted snapshot dataframe.
    timestamp: str, the column name of the timestamp column
    step: str, in format '2d3h4m5s', it means each timeframe is 2 days 3 hours 4 minutes and 5 seconds
    by: list of str, the pivot column names
    value_col: str, the value column name
    timeformat: str, the time format in start_time, end_time and timestamp columns
    fill: str, same with pd.DataFrame fillna 'backfill', 'bfill', 'pad', 'ffill', None
        which method to use to the none values in the pivot dataframe
    default: the default value in the pivot dataframe

    Returns
    -------
    pd.DataFrame: the pivot dataframe.
    """
    
    # Consolidate datetime inputs into datetime type
    start_time = start_time if isinstance(start_time, datetime) else datetime.strptime(start_time, timeformat)
    end_time = end_time if isinstance(end_time, datetime) else datetime.strptime(end_time, timeformat)
    df[timestamp] = pd.to_datetime(df[timestamp], format=timeformat)
    snapshot_columns = df.groupby(by).size().to_frame().transpose().columns

    # Construct the snapshot dataframe 
    step = _parse_step(step)
    timeframe = start_time
    snapshot_index = []
    while timeframe <= end_time:
        snapshot_index.append(timeframe)
        timeframe += step 
    snapshot_df = pd.DataFrame(np.empty((len(snapshot_index) - 1, len(snapshot_columns))), 
                               columns=snapshot_columns, 
                               index=snapshot_index[:-1])
    
    for i, s_e in enumerate(zip(snapshot_index[:-1], snapshot_index[1:])):
        s, e = s_e
        timeframe_series = _filter_within_range(df, timestamp, s, e).groupby(by).aggregate(agg_func)
        for value, cols in zip(timeframe_series[value_col], timeframe_series.index):
            snapshot_df.iloc[i][cols] = value
    return snapshot_df.fillna(method=fill).fillna(default)
            

def _filter_within_range(df, col, min, max):
    return df.loc[(df[col] >= min) & (df[col] <= max)].drop(columns=col)


def _parse_step(step):
    parse_regex = re.compile("((?P<d>[0-9]*)d)?((?P<h>[0-9]*)h)?((?P<m>[0-9]*)m)?((?P<s>[0-9]*)s)?")
    step_match = re.match(parse_regex, step)
    if step_match:
        return timedelta(days=int(step_match.group('d')) if step_match.group('d') else 0, 
                         hours=int(step_match.group('h')) if step_match.group('h') else 0, 
                         minutes=int(step_match.group('m')) if step_match.group('m') else 0,
                         seconds=int(step_match.group('s')) if step_match.group('s') else 0)
