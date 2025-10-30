### 1 Create Datasets
# downloading and saving data ready for zipline ingest
# 1.1 Imports

import os
import sys
import json
from datetime import date
import urllib.request
from io import StringIO

import pandas as pd

os.chdir(os.path.dirname(os.path.abspath(__file__)))


def create_constituents(df):
    # create Dataframe with current constituents
    ticker_list = []
    for i, row in df.iterrows():
        tmp_val = row['ticker']
        ticker_list.append(tmp_val)

    res_string = ','.join(ticker_list)

    results_df = pd.DataFrame({'date': [row['date'] if 'date' in row else date.today()],
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


def load_ticker_mappings():
    """
    Load ticker name mappings and deleted symbols from the JSON configuration file.
    Returns a tuple of (mappings_dict, deleted_symbols_set).
    All symbols are normalized to uppercase for case-insensitive matching.
    """
    config_file = 'ticker_name_mappings.json'
    
    try:
        with open(config_file, 'r', encoding='utf-8') as f:
            config = json.load(f)
            # Convert all mappings to uppercase for case-insensitive matching
            mappings = {k.upper(): v.upper() for k, v in config.get('mappings', {}).items()}
            # Convert deleted symbols to uppercase set for faster lookup
            deleted_symbols = {symbol.upper() for symbol in config.get('deleted_symbols', [])}
            return mappings, deleted_symbols
    except FileNotFoundError:
        print(f"Warning: Configuration file '{config_file}' not found!")
        print("Please create the configuration file or check the file path.")
        return {}, set()
    except json.JSONDecodeError as e:
        print(f"Error: Invalid JSON in configuration file '{config_file}': {e}")
        return {}, set()
    except Exception as e:
        print(f"Error loading configuration file '{config_file}': {e}")
        return {}, set()


def update_historical_ticker_names():
    """
    Update historical ticker symbols to their current names and remove deleted symbols.
    Creates a new file 'sp_500_current_name_with_historical_components.csv'
    
    This function:
    1. Updates ticker names (e.g., FB -> META, GOOG -> GOOGL)
    2. Removes deleted symbols (e.g., bankrupt companies, acquired companies)
    3. Removes redundant symbols within each date entry (e.g., if RTN->RTX and UTX->RTX, 
       only one RTX will remain for that date)
    4. Ensures all symbols are in uppercase format for consistency
    
    Ticker mappings and deleted symbols are loaded from 'ticker_name_mappings.json'
    """
    
    print("Loading ticker name mappings and deleted symbols from configuration file...")
    
    # Load ticker changes and deleted symbols from JSON configuration file
    ticker_changes, deleted_symbols = load_ticker_mappings()
    
    if not ticker_changes and not deleted_symbols:
        print("No ticker mappings or deleted symbols found. Please check your configuration file.")
        return None
    
    print(f"Loaded {len(ticker_changes)} ticker mappings and {len(deleted_symbols)} deleted symbols from configuration.")
    print("Updating historical ticker names to current names and removing deleted symbols...")
    
    # Read the historical components file
    try:
        df = pd.read_csv('sp_500_historical_components.csv')
    except FileNotFoundError:
        print("Error: sp_500_historical_components.csv not found!")
        return None
    
    if df.empty:
        print("Historical file is empty!")
        return None
    
    # Process each row and update ticker names
    updated_rows = []
    changes_made = 0
    deletions_made = 0
    redundant_symbols_removed = 0
    
    for idx, row in df.iterrows():
        date = row['date']
        tickers = row['tickers']
        
        # Split the tickers string into individual tickers
        ticker_list = [ticker.strip() for ticker in tickers.split(',')]
        original_count = len(ticker_list)
        
        # Update ticker names and remove deleted symbols
        updated_tickers = []
        row_changes = 0
        row_deletions = 0
        
        for ticker in ticker_list:
            # Convert ticker to uppercase for case-insensitive matching
            ticker_upper = ticker.upper()
            
            # Skip deleted symbols (case-insensitive)
            if ticker_upper in deleted_symbols:
                row_deletions += 1
                continue
                
            # Update ticker names if they exist in the mapping (case-insensitive)
            if ticker_upper in ticker_changes:
                updated_tickers.append(ticker_changes[ticker_upper])
                row_changes += 1
            else:
                # Keep original ticker (but ensure uppercase for consistency)
                updated_tickers.append(ticker_upper)
        
        # Remove duplicate symbols within the same date entry
        before_dedup_count = len(updated_tickers)
        updated_tickers = list(set(updated_tickers))
        after_dedup_count = len(updated_tickers)
        row_redundant_removed = before_dedup_count - after_dedup_count
        
        # Sort the updated tickers to maintain consistency
        updated_tickers.sort()
        
        # Only add the row if there are still tickers remaining after deletions
        if updated_tickers:
            updated_rows.append({
                'date': date,
                'tickers': ','.join(updated_tickers)
            })
        
        changes_made += row_changes
        deletions_made += row_deletions
        redundant_symbols_removed += row_redundant_removed
    
    # Create new DataFrame with updated ticker names
    updated_df = pd.DataFrame(updated_rows)
    
    # Remove any duplicate rows that might have been created by the name changes
    original_count = len(updated_df)
    updated_df = updated_df.drop_duplicates(subset=['date', 'tickers'], keep='first')
    dedupe_removed = original_count - len(updated_df)
    
    # Save to new file
    output_filename = 'sp_500_current_name_with_historical_components.csv'
    updated_df.to_csv(output_filename, index=False)
    
    # Report results
    print(f"Ticker name update and deletion completed:")
    print(f"- Original records: {len(df)}")
    print(f"- Records with updated ticker names: {len(updated_df)}")
    print(f"- Individual ticker name changes made: {changes_made}")
    print(f"- Individual ticker deletions made: {deletions_made}")
    print(f"- Redundant symbols removed (duplicates within same date): {redundant_symbols_removed}")
    print(f"- Duplicate records removed after updates: {dedupe_removed}")
    print(f"- Output saved as: {output_filename}")
    
    # Show what changes were made
    if changes_made > 0:
        print(f"\nTicker mappings applied:")
        for old_ticker, new_ticker in ticker_changes.items():
            print(f"  {old_ticker} -> {new_ticker}")
    
    if deletions_made > 0:
        print(f"\nDeleted symbols removed:")
        for symbol in deleted_symbols:
            print(f"  {symbol} (deleted)")
    
    return updated_df


def main():
    # read historical data
    sp500_hist = pd.read_csv('sp_500_historical_components.csv')

    # current companies
    sp_500_url = 'https://en.wikipedia.org/wiki/List_of_S%26P_500_companies'
    # Add User-Agent header to avoid 403 Forbidden error
    req = urllib.request.Request(sp_500_url, headers={'User-Agent': 'Mozilla/5.0'})
    html = urllib.request.urlopen(req).read()
    sp_500_constituents = pd.read_html(StringIO(html.decode('utf-8')), header=0)[0].rename(columns=str.lower)
    
    # Prepare the data for comparison (without saving yet)
    temp_constituents = sp_500_constituents.copy()
    temp_constituents.drop(['gics sector', 'gics sub-industry',
                            'headquarters location', 'date added',
                            'cik', 'founded', 'security'], axis=1, inplace=True)

    temp_constituents.columns = ['ticker']
    temp_constituents.sort_values(by='ticker', ascending=True, inplace=True)

    # Create new constituents string for comparison
    ticker_list = temp_constituents['ticker'].tolist()
    new_tickers = ','.join(ticker_list)
    
    # Check if the current constituents are different from the last record
    if not sp500_hist.empty:
        last_tickers = sp500_hist['tickers'].iloc[-1]
        if new_tickers == last_tickers:
            print(f"No changes in S&P 500 constituents since {sp500_hist['date'].iloc[-1]}. Files not updated.")
            return
        else:
            print(f"Changes detected in S&P 500 constituents. Updating files.")
    
    # Only generate files if there are changes or this is the first run
    # Now add the date and save the current constituents file
    sp_500_constituents['date'] = date.today()
    sp_500_constituents.to_csv('sp500_constituents.csv', index=False)
    
    # Prepare data for historical file
    temp_constituents['date'] = date.today()
    df = create_constituents(temp_constituents)
    
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
    elif len(sys.argv) > 1 and sys.argv[1] == '--update-names':
        update_historical_ticker_names()
    elif len(sys.argv) > 1 and sys.argv[1] == '--help':
        print("Usage:")
        print("  python sp500.py                  # Regular update (fetch current data)")
        print("  python sp500.py --clean          # Clean redundant historical data")
        print("  python sp500.py --update-names   # Update historical tickers to current names and remove deleted symbols")
        print("  python sp500.py --help           # Show this help message")
    else:
        main()
