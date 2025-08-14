# sp500_constituents

List of S&P 500 historical constituents from 1996/01/02 to present. CSV format where each date contains a list of the constituents.

## Description of Files

**'sp500_constituents.csv'** - Current S&P 500 composition from Wikipedia and output from **'sp500.py'**

**'sp_500_historical_components.csv'** - Contains historical S&P 500 index membership from 1996 to present. Output from **'sp500.py'**. This file has been optimized to remove redundant data and save storage space.

## Usage

### Regular Data Update

```bash
python sp500.py
```

This will fetch current S&P 500 constituents from Wikipedia and only update the historical file if there are actual changes to the index composition.

### Clean Redundant Historical Data

```bash
python sp500.py --clean
```

This will remove redundant consecutive entries with identical ticker compositions from the historical data file, keeping only the first occurrence of each unique composition change.

### Update Ticker Names

```bash
python sp500.py --update-names
```

This will update historical ticker symbols to their current names based on the mappings defined in `ticker_name_mappings.json`. This is useful for:

- **Company rebranding**: FB → META (Facebook to Meta Platforms)
- **Format standardization**: BRK-B → BRK.B (consistent ticker formatting)
- **Merger/acquisition tracking**: DISCA → WBD (Discovery to Warner Bros Discovery)

The function creates a new file `sp_500_current_name_with_historical_components.csv` with updated ticker names while preserving the original historical file.

### Configuration File

The ticker name mappings are stored in `ticker_name_mappings.json`:

```json
{
  "mappings": {
    "FB": "META",
    "BRKB": "BRK.B", 
    "BRK-B": "BRK.B",
    "BF-B": "BF.B",
    "KRFT": "KHC",
    "DISCA": "WBD"
  }
}
```

To add new ticker mappings:

1. Edit `ticker_name_mappings.json`
2. Add entries in the "mappings" section using format `"OLD_TICKER": "NEW_TICKER"`
3. Run `python sp500.py --update-names` to apply the changes

## Storage Optimization

The historical data file has been optimized to eliminate redundant entries:

- **Before optimization**: 3,471+ daily records with many duplicates
- **After optimization**: ~674 unique records representing actual composition changes
- **Storage reduction**: ~80% file size reduction (from 6.4MB to 1.2MB)
- **Data integrity**: All meaningful S&P 500 composition changes are preserved

The optimization automatically creates a backup file (`sp_500_historical_components_backup.csv`) before cleaning.

## Features

- **Intelligent Updates**: Only updates historical file when S&P 500 composition actually changes
- **Storage Efficient**: Removes redundant daily duplicates while preserving all composition changes
- **Safe Operations**: Creates automatic backups before data cleanup
- **Historical Accuracy**: Maintains complete record of all S&P 500 membership changes since 1996

