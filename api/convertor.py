import pandas as pd
import numpy as np

class DatabentoConverter:
    """
    A modular class to convert Databento MBP-1 data into the format required for R analysis.
    """

    def __init__(self):
        pass

    def convert(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Convert Databento MBP-1 data into the required R-compatible top-of-book format:
        - One row per unique (UpdateTime, UpdateMillisec) timestamp.
        - For each timestamp, find the best bid (max price) & sum volume, 
          and the best ask (min price) & sum volume.
        - Fill LastPrice, Volume, and Turnover with NaN (since MBP-1 is quote-only).

        Returns a DataFrame with columns:
          [UpdateTime, UpdateMillisec, LastPrice, Volume, Turnover,
           BidPrice1, BidVolume1, AskPrice1, AskVolume1, SecondOfDay].
        """

        # Ensure ts_event is a valid datetime
        df["ts_event"] = pd.to_datetime(df["ts_event"], errors="coerce")

        # Extract UpdateTime and UpdateMillisec from ts_event
        df["UpdateTime"] = df["ts_event"].dt.strftime("%H:%M:%S")
        df["UpdateMillisec"] = df["ts_event"].dt.microsecond // 1000

        # Compute SecondOfDay (time since midnight in seconds)
        df["SecondOfDay"] = (
            df["ts_event"].dt.hour * 3600 +
            df["ts_event"].dt.minute * 60 +
            df["ts_event"].dt.second
        )

        # Sort by timestamp to keep chronological order
        df.sort_values(["ts_event"], inplace=True)

        # Group by the exact (UpdateTime, UpdateMillisec, SecondOfDay)
        grouped = df.groupby(["UpdateTime", "UpdateMillisec", "SecondOfDay"])

        records = []
        for (u_time, u_millis, sec_of_day), group_data in grouped:
            # Separate bids and asks within this group
            bids_df = group_data[group_data["side"] == "B"]
            asks_df = group_data[group_data["side"] == "S"]

            # Find best bid (max price) and sum volumes at that price
            if not bids_df.empty:
                best_bid_price = bids_df["price"].max()
                best_bid_size = bids_df.loc[bids_df["price"] == best_bid_price, "size"].sum()
            else:
                best_bid_price = np.nan
                best_bid_size = 0

            # Find best ask (min price) and sum volumes at that price
            if not asks_df.empty:
                best_ask_price = asks_df["price"].min()
                best_ask_size = asks_df.loc[asks_df["price"] == best_ask_price, "size"].sum()
            else:
                best_ask_price = np.nan
                best_ask_size = 0

            # Build your final record
            record = {
                "UpdateTime": u_time,
                "UpdateMillisec": u_millis,
                # MBP-1 is quote only, so these remain NaN
                "LastPrice": np.nan,
                "Volume": np.nan,
                "Turnover": np.nan,
                "BidPrice1": best_bid_price,
                "BidVolume1": best_bid_size,
                "AskPrice1": best_ask_price,
                "AskVolume1": best_ask_size,
                "SecondOfDay": sec_of_day
            }
            records.append(record)

        # Create a new DataFrame from the aggregated records
        final_df = pd.DataFrame(records)

        # Reorder columns to match your original approach
        final_df = final_df[
            [
                "UpdateTime", "UpdateMillisec", "LastPrice", "Volume", "Turnover",
                "BidPrice1", "BidVolume1", "AskPrice1", "AskVolume1", "SecondOfDay"
            ]
        ]

        # If desired, you can forward-fill any missing quotes from previous timestamps:
        # final_df[["BidPrice1", "BidVolume1", "AskPrice1", "AskVolume1"]] = \
        #     final_df[["BidPrice1", "BidVolume1", "AskPrice1", "AskVolume1"]].ffill()

        return final_df
