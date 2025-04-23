import schedule
import time
import subprocess

def run_etl():
    print("Running ETL script...")
    subprocess.run(["python", "olist_etl_final.py"])

# Run the ETL script immediately
run_etl()

# Schedule: Run ETL every day at 2 AM
schedule.every(1).minutes.do(run_etl)  # Run every 1 minute


while True:
    schedule.run_pending()
    time.sleep(60)  # Check every 60 seconds
