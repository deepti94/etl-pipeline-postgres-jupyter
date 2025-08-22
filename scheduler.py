from apscheduler.schedulers.blocking import BlockingScheduler
import logging
import main  

# Set up logging
logging.basicConfig(
    filename="pipeline/scheduler.log",
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s"
)

# Scheduled job
def scheduled_job():
    logging.info("Scheduler job started.")
    try:
        main.run_pipeline()
        logging.info("Scheduler job completed successfully.")
    except Exception as e:
        logging.error(f"Scheduler job failed: {e}")

# Set up scheduler
if __name__ == "__main__":
    scheduler = BlockingScheduler()
    scheduler.add_job(scheduled_job, 'interval', seconds=60)  # ⏱️ Every 60 seconds
    logging.info("Scheduler started. Running every 60 seconds.")

    try:
        scheduler.start()
    except (KeyboardInterrupt, SystemExit):
        logging.info("Scheduler stopped.")
