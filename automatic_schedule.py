import schedule
import time
import threading

def schedule_days(ingest_function, interval_in_days):
    def run_schedule():
        schedule.every(interval_in_days).days.do(ingest_function)
        
        while True:
            schedule.run_pending()
            time.sleep(1440)  
    # Start the scheduling function in a separate thread
    scheduling_thread = threading.Thread(target=run_schedule)
    scheduling_thread.daemon = True  # This allows the thread to exit when the main program finishes
    scheduling_thread.start()

def schedule_minutes(ingest_function, interval_in_minutes):
    def run_schedule():
        schedule.every(interval_in_minutes).minutes.do(ingest_function)
        
        while True:
            schedule.run_pending()
            time.sleep(60)  # Sleep for 60 seconds before checking again
    
    # Start the scheduling function in a separate thread
    scheduling_thread = threading.Thread(target=run_schedule)
    scheduling_thread.daemon = True  # This allows the thread to exit when the main program finishes
    scheduling_thread.start()
