import databento as db
import configparser
import os
import pandas as pd
import time  # optional, for delaying API calls

from abc import ABC, abstractmethod
#from api.convertor import DatabentoConverter


class BaseDownloader(ABC):
    @abstractmethod
    def fetch_data(self, dataset, symbol, start_date, end_date, schemas):
        """Abstract method to fetch data for a given date range."""
        pass

    @abstractmethod
    def save_data(self, df, filename):
        """Abstract method to save fetched data."""
        pass


class DataBentoDownloader(BaseDownloader):
    def __init__(self, config_path="api/databento/creds.ini"):
        """Initialize downloader and load API credentials."""
        self.config = configparser.ConfigParser()
        self.config.read(config_path)
        self.api_key = self.config["Databento"]["API_KEY"]
        self.client = db.Historical(self.api_key)

        # Our converter handles MBP-1 data. If you want to handle trades,
        # you'd either use a second converter or incorporate logic in the same converter.
        #self.converter = DatabentoConverter()
        
        # Define base paths for data storage
        self.raw_data_path = os.path.join("data", "databento", "raw")
        self.processed_data_path = os.path.join("data", "databento")
        
        # Ensure directories exist
        os.makedirs(self.raw_data_path, exist_ok=True)
        os.makedirs(self.processed_data_path, exist_ok=True)

    def fetch_data(self,
                   dataset="GLBX.MDP3",
                   symbol="ESH4",
                   start_date="2024-03-02",
                   end_date="2024-03-03",
                   schemas=None,
                   delay_seconds=0,
                   force_download=False):
        """
        Fetch data for multiple schemas, day by day, and save combined results.
        schemas: list of schema strings (e.g. ['mbp-1', 'trades']).
        Now saves raw data files before combining.
        
        Parameters:
        -----------
        dataset : str
            The dataset to fetch from (e.g., "GLBX.MDP3")
        symbol : str
            The symbol to fetch data for (e.g., "ESH4")
        start_date : str
            Start date in format "YYYY-MM-DD"
        end_date : str
            End date in format "YYYY-MM-DD"
        schemas : list
            List of schemas to fetch (e.g., ["mbp-1", "trades"])
        delay_seconds : int
            Delay between API calls in seconds
        force_download : bool
            If True, download data even if it already exists locally
        """

        if schemas is None:
            # Default to just one schema if none provided
            schemas = ["trades"]

        date_range = pd.date_range(start=start_date, end=end_date, freq="D")
        
        for date_obj in date_range:
            date_str = date_obj.strftime("%Y-%m-%d")
            print(f"\n--- Processing {symbol} on {date_str} ---")
            
            # Check if final combined data already exists
            final_filename = f"{symbol.lower()}_{date_str}.csv"
            final_filepath = os.path.join(self.processed_data_path, final_filename)
            
            if os.path.exists(final_filepath) and not force_download:
                print(f"Final combined data for {date_str} already exists. Skipping download.")
                continue
                
            # Check if all raw data files exist
            all_raw_exist = True
            for schema in schemas:
                raw_filename = f"{symbol.lower()}_{schema.replace('-', '')}_{date_str}.csv"
                raw_filepath = os.path.join(self.raw_data_path, raw_filename)
                
                if not os.path.exists(raw_filepath):
                    all_raw_exist = False
                    break
            
            # If all raw files exist but final doesn't, just process the raw files
            if all_raw_exist and not force_download:
                print(f"All raw data files for {date_str} exist. Processing without downloading.")
                self.process_raw_data(symbol, date_str, schemas)
                continue

            daily_dfs = {}
            for schema in schemas:
                print(f"Fetching schema='{schema}' for date={date_str}...")
                
                # Check if this specific raw file exists
                raw_filename = f"{symbol.lower()}_{schema.replace('-', '')}_{date_str}.csv"
                raw_filepath = os.path.join(self.raw_data_path, raw_filename)
                
                if os.path.exists(raw_filepath) and not force_download:
                    print(f"Raw {schema} data for {date_str} already exists. Loading from file.")
                    df_raw = pd.read_csv(raw_filepath)
                    daily_dfs[schema] = df_raw
                else:
                    try:
                        data = self.client.timeseries.get_range(
                            dataset=dataset,
                            symbols=[symbol],
                            schema=schema,
                            start=f"{date_str}T00:00:00Z",
                            end=f"{date_str}T23:59:59Z",
                        )
                        df_raw = data.to_df()
                        daily_dfs[schema] = df_raw
                        
                        # Save raw data for this schema
                        self.save_raw_data(df_raw, raw_filename)
                        print(f"Raw {schema} data saved for {date_str}")

                    except Exception as e:
                        print(f"Error fetching {schema} for {date_str}: {e}")
                        
                        # Try to load from previously saved raw data if available
                        if os.path.exists(raw_filepath):
                            print(f"Loading previously saved raw data for {schema} on {date_str}")
                            df_raw = pd.read_csv(raw_filepath)
                            daily_dfs[schema] = df_raw
                        else:
                            daily_dfs[schema] = pd.DataFrame()

                if delay_seconds:
                    time.sleep(delay_seconds)
            
            # Combine data from multiple schemas
            try:
                df_combined = self._combine_schemas(daily_dfs)
                
                # Save final combined data
                self.save_data(df_combined, final_filename)
                print(f"Combined data saved for {date_str}")
            except Exception as e:
                print(f"Error combining schemas for {date_str}: {e}")
                print("Raw data is still available for manual processing.")

    def check_data_exists(self, symbol, date_str, schemas=None):
        """
        Check if data already exists for the given symbol and date.
        
        Returns:
        --------
        dict
            A dictionary with keys 'final' (bool) and 'raw' (dict of schema: bool)
            indicating which files exist
        """
        if schemas is None:
            schemas = ["mbp-1", "trades"]
            
        result = {
            'final': False,
            'raw': {schema: False for schema in schemas}
        }
        
        # Check final combined file
        final_filename = f"{symbol.lower()}_{date_str}.csv"
        final_filepath = os.path.join(self.processed_data_path, final_filename)
        result['final'] = os.path.exists(final_filepath)
        
        # Check raw files for each schema
        for schema in schemas:
            raw_filename = f"{symbol.lower()}_{schema.replace('-', '')}_{date_str}.csv"
            raw_filepath = os.path.join(self.raw_data_path, raw_filename)
            result['raw'][schema] = os.path.exists(raw_filepath)
            
        return result

    def save_raw_data(self, df, filename):
        """Save raw data for a specific schema."""
        if df.empty:
            print(f"No data to save for {filename}")
            return
            
        output_path = os.path.join(self.raw_data_path, filename)
        df.to_csv(output_path, index=False)
        print(f"Raw data saved to {output_path}")

    def _combine_schemas(self, daily_dfs: dict) -> pd.DataFrame:
        """
        Combine data from multiple schemas into a single DataFrame.
        Expected format for combined output:
        UpdateTime,UpdateMillisec,LastPrice_x,Volume_x,Turnover,BidPrice1,BidVolume1,AskPrice1,AskVolume1,SecondOfDay,ts_event,LastPrice_y,Volume_y,ts_event
        """
        # Check if we have the required schemas
        if 'mbp-1' not in daily_dfs or daily_dfs['mbp-1'].empty:
            raise ValueError("MBP-1 data is required but not available")
            
        if 'trades' not in daily_dfs or daily_dfs['trades'].empty:
            raise ValueError("Trades data is required but not available")
            
        # Process MBP-1 data
        mbp_df = daily_dfs['mbp-1'].copy()
        
        # Extract time components from ts_event for MBP data
        mbp_df['ts_datetime'] = pd.to_datetime(mbp_df['ts_event'])
        mbp_df['UpdateTime'] = mbp_df['ts_datetime'].dt.strftime('%H:%M:%S')
        mbp_df['UpdateMillisec'] = mbp_df['ts_datetime'].dt.microsecond // 1000
        
        # Calculate seconds of day
        seconds_of_day = (mbp_df['ts_datetime'].dt.hour * 3600 + 
                          mbp_df['ts_datetime'].dt.minute * 60 + 
                          mbp_df['ts_datetime'].dt.second)
        mbp_df['SecondOfDay'] = seconds_of_day
        
        # Extract bid/ask data
        mbp_df['BidPrice1'] = mbp_df['bid_px_00']
        mbp_df['BidVolume1'] = mbp_df['bid_sz_00']
        mbp_df['AskPrice1'] = mbp_df['ask_px_00']
        mbp_df['AskVolume1'] = mbp_df['ask_sz_00']
        
        # Process trades data
        trades_df = daily_dfs['trades'].copy()
        trades_df['ts_datetime'] = pd.to_datetime(trades_df['ts_event'])
        
        # Extract price and volume from trades
        trades_df['LastPrice_y'] = trades_df['price']
        trades_df['Volume_y'] = trades_df['size']
        
        # Merge the dataframes on closest timestamp
        # First, create a synthetic timestamp for both dataframes
        mbp_df['synthetic_ts'] = self._rebuild_timestamp(mbp_df)
        trades_df['synthetic_ts'] = self._rebuild_timestamp(trades_df)
        
        # Sort both dataframes by timestamp
        mbp_df = mbp_df.sort_values('synthetic_ts')
        trades_df = trades_df.sort_values('synthetic_ts')
        
        # Forward fill the MBP data to match with trades
        # This is a simplified approach - for production you might want a more sophisticated matching
        combined_df = pd.merge_asof(
            trades_df[['synthetic_ts', 'LastPrice_y', 'Volume_y', 'ts_event']],
            mbp_df[['synthetic_ts', 'UpdateTime', 'UpdateMillisec', 'BidPrice1', 'BidVolume1', 
                    'AskPrice1', 'AskVolume1', 'SecondOfDay', 'ts_event']],
            on='synthetic_ts',
            direction='backward'
        )
        
        # Add placeholder columns for the final format
        combined_df['LastPrice_x'] = combined_df['LastPrice_y']  # Placeholder
        combined_df['Volume_x'] = combined_df['Volume_y']  # Placeholder
        combined_df['Turnover'] = combined_df['LastPrice_y'] * combined_df['Volume_y']
        
        # Reorder columns to match target format
        final_columns = [
            'UpdateTime', 'UpdateMillisec', 'LastPrice_x', 'Volume_x', 'Turnover',
            'BidPrice1', 'BidVolume1', 'AskPrice1', 'AskVolume1', 'SecondOfDay',
            'ts_event_y', 'LastPrice_y', 'Volume_y', 'ts_event_x'
        ]
        
        # Rename columns to match expected format
        combined_df = combined_df.rename(columns={
            'ts_event_x': 'ts_event',
            'ts_event_y': 'ts_event_mbp'
        })
        
        # Select and reorder columns that exist
        available_columns = [col for col in final_columns if col in combined_df.columns]
        return combined_df[available_columns]

    def _rebuild_timestamp(self, df):
        """
        Create a synthetic timestamp from [UpdateTime, UpdateMillisec, SecondOfDay].
        Because aggregator only provides HH:MM:SS + ms + second_of_day,
        we can create a unified datetime for merging.
        For consistency, we'll assume the date is not known here, so we use a dummy date 
        or just the time-of-day. 
        """
        # If the dataframe already has ts_datetime, use that
        if 'ts_datetime' in df.columns:
            return df['ts_datetime']
            
        # Otherwise, try to build from components
        if 'UpdateTime' in df.columns and 'UpdateMillisec' in df.columns:
            combined_str = (
                df["UpdateTime"].astype(str)
                + "."
                + df["UpdateMillisec"].astype(int).astype(str).str.zfill(3)
            )
            # Now parse that as a time. We'll pick an arbitrary date, e.g. 1970-01-01
            synthetic_ts = pd.to_datetime("1970-01-01 " + combined_str, errors="coerce")
            return synthetic_ts
        
        # If we have ts_event, parse that
        if 'ts_event' in df.columns:
            return pd.to_datetime(df['ts_event'])
            
        # Fallback
        return pd.Series([pd.NaT] * len(df))

    def save_data(self, df, filename):
        """Save final combined DataFrame."""
        if df.empty:
            print(f"No data to save for {filename}")
            return
            
        output_path = os.path.join(self.processed_data_path, filename)
        df.to_csv(output_path, index=False)
        print(f"Combined data saved to {output_path}")
        
    def process_raw_data(self, symbol, date_str, schemas=None):
        """
        Process previously downloaded raw data files to create combined output.
        Useful as a fallback when real-time conversion fails.
        """
        if schemas is None:
            schemas = ["mbp-1", "trades"]
            
        print(f"\n--- Processing raw data for {symbol} on {date_str} ---")