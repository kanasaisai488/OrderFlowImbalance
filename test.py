import pandas as pd
import os
from api.convertor import DatabentoConverter

def test_converter():
    """
    Test the DatabentoConverter by loading an existing CSV file,
    converting it, and displaying the transformed data.
    """
    # Prompt user for filename
    filename = "esh4_mbo_2024-03-03.csv"
    file_path = os.path.join("data", "databento", filename)
    
    if not os.path.exists(file_path):
        print(f"File not found: {file_path}")
        return
    
    # Load CSV file
    df = pd.read_csv(file_path, parse_dates=["ts_event"])
    
    # Initialize the converter
    converter = DatabentoConverter()
    
    # Convert data
    transformed_df = converter.convert(df)
    
    # Display the transformed DataFrame
    print("\nTransformed Data:")
    print(transformed_df.head())
    
    # Save the transformed data to a new file
    output_filename = filename.replace(".csv", "_converted.csv")
    output_path = os.path.join("data", "databento", output_filename)
    transformed_df.to_csv(output_path, index=False)
    print(f"\nConverted file saved to: {output_path}")

if __name__ == "__main__":
    test_converter()
