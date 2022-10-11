import numpy as np
import pandas as pd


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
