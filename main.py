
"""
This piece of code is used to ingest all the CSV file data in various folders
into the AWS architecture.

Author - Shravan Aras <shravanaras@arizona.edu>
Date - 9/10/2024
"""

import argparse
import os
import re
import pandas as pd
import awswrangler as wr
import configparser

def looper(dirpath : str, config : dict):
    """
    Parse through all the files inside to the directory to start ingesting them.

    Paramters
    ---------
    1. dirpath (str) - Top level directory path.
    """
    dir_name_active = r'^PPID [0-9]+ Data$'
    dir_name_withdrew = r'^PPID [0-9]+ Data - withdrew$'
    directories = os.listdir(dirpath)
    for directory in directories:
        if not os.path.isdir(os.path.join(dirpath, directory)):
            print(f'Warning : {directory} is not a directory.')

        # Check if this is an active participant or has withdrawn from study.
        match_active = re.match(dir_name_active, directory)
        match_withdrawn = re.match(dir_name_withdrew, directory)

        if match_withdrawn:
            print(f'Log : Skipping {directory} as its been marked as withdrawn.')
            continue
        elif match_active:
            participantIngest(os.path.join(dirpath, directory), config)
        else:
            print(f'Warning : Directory name {directory} does not follow naming convention. Skipping it.')

def participantIngest(subdir : str, config : dict):
    """
    Method which reads all the files inside the participant sub-directory and uploads data to the appropriate
    tables on AWS Athena. It creates those tables if they don't exist.
    Directory names must follow the following structure - "tag1_tag2_tagN_pid_tablename.csv".
    There can be an arbitary number of tags before the pid.

    Parameters
    ----------
    1. subdir (str) - Participant sub-directory
    """
    file_name = r'^(\w+)_(\w+)_(\w+)_([0-9a-zA-Z \-]+)\.csv$'
    files = os.listdir(subdir)
    for file in files:
        match = re.match(file_name, file)
        if match:
            pid = match.group(3)
            tablename = match.group(4)
            # Replace space and - in table names with _
            tablename = tablename.replace(' ', '_')
            tablename = tablename.replace('-', '_')
            # If the table name is part of whitelist, then we can go ahead and ingest it.
            if tablename in config['whitelist_tables']:
                data = pd.read_csv(open(os.path.join(subdir, file), 'r'))
                ingestData(data, pid, tablename, config)
            #print(f'Matched pid - {pid} and tablename - {tablename}')
        else:
            print(f'Warning : File name {file} does not follow naming convention. Skipping it.')

def ingestData(data : pd.DataFrame, pid : str,
               tablename : str, config : dict,
               enableTyper = True):
    """
    Method which takes the data frame and then ingests it into AWS Athena.

    Parameters
    ----------
    1. data (pd.DataFrame) - The data frame to ingest.
    2. pid (str) - Participant Identifier.
    3. tablename (str) - Table name to upload the data to.
    4. enableTyper (True) - If true it will automatically try to infer types changes
                        to make it more suitable for Athena.
    """

    # Taking a look at the data types automatically inferred by pandas.
    print(f'Log : Ingesting Tablename - {tablename} for pid {pid}')
    s3_path = config['aws']['aws_s3_data']
    aws_database = config['aws']['aws_database']
    table_path = f'{s3_path}/{tablename}/'
    if s3_path[-1] == '/' : # Avoid adding the additional / if the path already ended in it.
        table_path = f'{s3_path}{tablename}/'

    # Add a new pid column to the data.
    data['pid'] = pid

    # If smartTyper is turned on then we will try to automatically infer types.
    # TODO: For now to prevent it from taking too long, this lookup is manual. Change this
    # to a more automated + cached lookup.
    if enableTyper:
        if tablename == 'activity' or tablename == 'sleep':
            data = smartTyper(sampleEntry(data['summary_date']), data, 'summary_date', True)
        if tablename == 'sleep_periods':
            data = smartTyper(sampleEntry(data['day']), data, 'day', True)

    # We are now ready to push all of this up to AWS.
    try:
        wr.s3.to_parquet(
            df=data,
            path=table_path,
            dataset=True,
            database=aws_database,
            table=tablename,
            partition_cols=['pid']
        )
    except Exception as e:
        print(f'Error : Could not create table {tablename} on AWS.')

def showDataTypes(data : pd.DataFrame):
    """
    Method which prints out all the data types for the pandas frame.

    Parameters
    ----------
    1. data (pd.DataFrame) - Data Frame.
    """
    columns = data.columns
    for c in columns:
        sample = sampleEntry(data[c])
        print(f'{c} --> {data[c].dtypes} --> {sample}')
    print('')

def smartTyper(example : str, df : pd.DataFrame, col : str, convert = False):
    """
    This method takes an example of how the data (which is usually typed as a string)
    looks like and suggests some pandas friendly data types that can be used.
    Helps with eventual conversion from awswrangler.
    Note - Because this uses regular expression as its core, it only does format matching
            and not *validation* for correctness.

    Parameter
    ---------
    1. example (str) : An example of the data we want to infer.
    2. df (pd.DataFrame) : Pandas frame that we want to apply the smart type to.
    3. col (str) : The column that we want to change the type of.
    4. inplace (False) : If the operations must be performed in place or a new series returned.

    Returns
    -------
    1. convert (False) : By default this only returns the columsn modified with the new datatype
                    but if set to True then it will update the df to reflect those changes.
    """
    if not (type(example) == type('hello')):
        return type(str)

    date_exp = r'\b(\d{4})-(0[1-9]|1[0-2])-(0[1-9]|1[0-9]|2[0-9]|3[01])\b'

    try:
        if re.match(date_exp, example):
            useries = pd.to_datetime(df[col])
            print(f'Log : Automatically updated "{col}" data type from "{type(example)}" to "{useries.dtype}"')
            if convert:
                df[col] = useries
                return df
            else:
                return useries
    except ValueError as ve:
        print(f'Warning : Automatic conversion of type failed for {col} with example value {example} with the error - {ve}')
        return None

def sampleEntry(column : pd.Series) -> object:
    """
    Method which randomly samples an entry from the given series to show as an example

    Parameters
    ----------
    1. column (pd.Series) - The column values to samples from.

    Returns
    -------
    1. An example entry from the column.
    """

    sample = None
    try:
        sample = column[0]
    except:
        pass
    return sample

def loadConfig():
    """
    Method which loads all the configuration information required from various local files.
    """

    whitelist_file = 'whitelist.txt'

    whitelist_tables = []
    with open(whitelist_file, 'r') as f:
        for line in f.readlines():
            whitelist_tables.append(line.strip())

    config = configparser.ConfigParser()
    config.read('config.cfg')
    aws = config['AWS']

    if not ('aws_database' in aws and 'aws_s3_data' in aws):
        raise Exception('Malformed config file section - aws')

    config = {
        'whitelist_tables' : whitelist_tables,
        'aws' : {
            'aws_database' : aws['aws_database'],
            'aws_s3_data' : aws['aws_s3_data']
        }
    }

    return config

def main():
    # Parse command line arguments.
    parser = argparse.ArgumentParser(description='Read CSV contents to add to AWS database.')
    parser.add_argument('directory')
    args = parser.parse_args()

    config = loadConfig()
    looper(args.directory, config)

if __name__ == '__main__':
    main()
