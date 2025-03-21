# main.py

from api.downloader import DataBentoDownloader

def main():
    print("Starting Databento data retrieval...")
    downloader = DataBentoDownloader()

    # Example: we want both MBP-1 quotes and trades in the same final CSV
    schemas = ["mbp-1", "trades"]
    #schemas = ["trades"]

    downloader.fetch_data(
        dataset="GLBX.MDP3",
        symbol="RBM5",
        start_date="2025-01-01",
        end_date="2025-03-31",
        schemas=schemas,
        delay_seconds=1
    )

    print("Data retrieval completed!")

if __name__ == "__main__":
    main()

