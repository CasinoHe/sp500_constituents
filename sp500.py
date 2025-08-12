### 1 Create Datasets
# downloading and saving data ready for zipline ingest
# 1.1 Imports

import os
import sys
from datetime import date

import pandas as pd

os.chdir(os.path.dirname(os.path.abspath(__file__)))


def create_constituents(df):
    # create Dataframe with current constituents
    ticker_list = []
    for i, row in df.iterrows():
        tmp_val = row['ticker']
        ticker_list.append(tmp_val)

    res_string = ','.join(ticker_list)

    results_df = pd.DataFrame({'date': date.today(),
                               'tickers': [res_string],
                               })

    return results_df


def clean_historical_data():
    """
    Remove redundant consecutive entries with identical ticker lists from historical data.
    Keep only the first occurrence of each unique ticker composition.
    """
    # Read the historical data
    df = pd.read_csv('sp_500_historical_components.csv')
    
    if df.empty:
        print("Historical file is empty, nothing to clean.")
        return
    
    # Convert date column to datetime for proper sorting
    df['date'] = pd.to_datetime(df['date'])
    df = df.sort_values('date')
    
    # Keep track of unique ticker combinations
    cleaned_df = []
    prev_tickers = None
    
    for idx, row in df.iterrows():
        current_tickers = row['tickers']
        
        # Keep the record if it's different from the previous one or if it's the first record
        if current_tickers != prev_tickers:
            cleaned_df.append(row)
            prev_tickers = current_tickers
    
    # Create cleaned dataframe
    cleaned_df = pd.DataFrame(cleaned_df)
    
    # Convert date back to string format (YYYY-MM-DD)
    cleaned_df['date'] = cleaned_df['date'].dt.strftime('%Y-%m-%d')
    
    # Calculate how many records were removed
    original_count = len(df)
    cleaned_count = len(cleaned_df)
    removed_count = original_count - cleaned_count
    
    # Create backup of original file
    backup_filename = 'sp_500_historical_components_backup.csv'
    df['date'] = df['date'].dt.strftime('%Y-%m-%d')  # Convert back for backup
    df.to_csv(backup_filename, index=False)
    
    # Save cleaned data
    cleaned_df.to_csv('sp_500_historical_components.csv', index=False)
    
    print(f"Historical data cleanup completed:")
    print(f"- Original records: {original_count}")
    print(f"- Cleaned records: {cleaned_count}")
    print(f"- Removed redundant records: {removed_count}")
    print(f"- Backup saved as: {backup_filename}")
    
    return cleaned_df


def main():
    # read historical data
    sp500_hist = pd.read_csv('sp_500_historical_components.csv')

    # current companies
    sp_500_url = 'https://en.wikipedia.org/wiki/List_of_S%26P_500_companies'
    sp_500_constituents = pd.read_html(sp_500_url, header=0)[0].rename(columns=str.lower)
    sp_500_constituents['date'] = date.today()
    sp_500_constituents.to_csv('sp500_constituents.csv', index=False)
    sp_500_constituents.drop(['gics sector', 'gics sub-industry',
                              'headquarters location', 'date added',
                              'cik', 'founded', 'security'], axis=1, inplace=True)

    sp_500_constituents.columns = ['ticker', 'date']
    sp_500_constituents.sort_values(by='ticker', ascending=True,inplace=True)

    # Create new constituents string
    df = create_constituents(sp_500_constituents)
    new_tickers = df['tickers'].iloc[0]
    
    # Check if the current constituents are different from the last record
    if not sp500_hist.empty:
        last_tickers = sp500_hist['tickers'].iloc[-1]
        if new_tickers == last_tickers:
            print(f"No changes in S&P 500 constituents since {sp500_hist['date'].iloc[-1]}. Historical file not updated.")
            return
        else:
            print(f"Changes detected in S&P 500 constituents. Updating historical file.")
    
    # If there are changes or this is the first run, append the new record
    final = pd.concat([sp500_hist, df], ignore_index=True)

    # output
    final = final.drop_duplicates(subset=['date', 'tickers'],keep='last')
    final.to_csv('sp_500_historical_components.csv', index=False)
    print(f"Historical components file updated with data for {date.today()}")
    

if __name__ == '__main__':
    # Check if user wants to clean historical data
    if len(sys.argv) > 1 and sys.argv[1] == '--clean':
        clean_historical_data()
    else:
        main()
