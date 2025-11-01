import pandas as pd
import glob, os
from tqdm.auto import tqdm
from ROOT import RooRealVar, RooArgSet, RooDataSet

def load_data(base_path, merge = False, print_all_files = False, **pattern_elements):
    all_files = glob.glob(os.path.join(base_path, '*'))
    files_found = [f for f in all_files if all(str(v) in os.path.basename(f) for v in pattern_elements.values())]
    if print_all_files:
        print(files_found)
    print(f'Found {len(files_found)} files')
    files_loaded = {os.path.basename(file_path): None for file_path in files_found}
    print(len(files_loaded))
    for file_path in tqdm(files_found, desc='Loading files', total=len(files_found)):
        try:
            df_tmp = pd.read_csv(file_path)
            for pattern_key, pattern_value in pattern_elements.items():
                df_tmp[pattern_key] = pattern_value
            files_loaded[os.path.basename(file_path)] = df_tmp
        except Exception as e:
            print(f'[ERROR] Can not open file {file_path}: \n{e}')

    if merge:
        resulting_df = pd.DataFrame()
        for file_name, file in tqdm(files_loaded.items(), desc='Merging files', total=len(files_loaded)):
            file['filesource'] = file_name
            try:
                resulting_df = pd.concat([resulting_df, file], axis = 0)
            except:
                print(f'[ERROR] File {file_name} has not been added to resulting dataframe!')
                continue
        return resulting_df
    else:
        return files_loaded

def remove_duplicate_rows(data: pd.DataFrame, check_variables: list = None) -> pd.DataFrame:
    '''
    Removes duplicate rows based on specified variables.
    
    Args:
        data: Input DataFrame
        check_variables: List of column names to check for duplicates. 
                        If None, uses all columns.
    
    Returns:
        DataFrame without duplicates based on the specified variables
    '''
    if check_variables is None:
        check_variables = data.columns.tolist()
    
    initial_count = len(data)
    
    # Remove duplicates (keeps first occurrence by default)
    cleaned_data = data.drop_duplicates(subset=check_variables)
    
    final_count = len(cleaned_data)
    duplicates_removed = initial_count - final_count
    
    print(f'Duplications before removal: {duplicates_removed/initial_count*100:.1f}%')
    print(f'Rows removed: {duplicates_removed}')
    print(f'Duplications after removal: 0.0%')
    
    return cleaned_data



def check_columns_exist(columns_to_check: set, *dataframes: pd.DataFrame) -> bool:
    '''
    Checks if all specified columns exist in every provided DataFrame.
    
    Args:
        columns_to_check: Set of column names that must be present
        *dataframes: Variable number of pandas DataFrames to check
    
    Returns:
        True if all columns exist in all DataFrames, False otherwise
    '''
    if not dataframes:
        return False

    columns_to_check = set(columns_to_check)
    
    for df in dataframes:
        if not columns_to_check.issubset(set(df.columns)):
            return False
    
    return True

def create_roodataset_from_dataframe(df: pd.DataFrame, data_columns: 'list[str] | None' = None) -> RooDataSet:
    """
    Create a RooDataSet from a pandas DataFrame using the specified columns.

    Args:
        df (pd.DataFrame): The DataFrame containing the data.
        data_columns (list of str, optional): The columns to include in the RooDataSet.
            If None, all columns of the dataframe are used.

    Returns:
        RooDataSet: The constructed RooDataSet.
    """
    if data_columns is None:
        data_columns = df.columns

    roo_vars = [RooRealVar(
        var,
        var,
        float(df[var].min()),
        float(df[var].max())
    ) for var in data_columns if var in df.columns]

    roo_varset = RooArgSet()
    for rv in roo_vars:
        roo_varset.add(rv)

    roodataset = RooDataSet("ds", "ds", roo_varset)

    for idx, row in tqdm(df.iterrows(), total=len(df)):
        for rv in roo_vars:
            rv.setVal(row[rv.GetName()])
        roodataset.add(roo_varset)

    return roodataset