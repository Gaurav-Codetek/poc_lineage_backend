from apscheduler.schedulers.background import BackgroundScheduler
from pytz import timezone

from app.services.refresh_service import background_ingestion


def start_scheduler():
    scheduler = BackgroundScheduler(timezone=timezone("Asia/Kolkata"))
    scheduler.add_job(background_ingestion, trigger="cron", hour=0, minute=0)
    scheduler.start()
    print("Midnight IST ingestion scheduler started.")
