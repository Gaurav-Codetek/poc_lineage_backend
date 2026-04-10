from apscheduler.schedulers.background import BackgroundScheduler
from pytz import timezone

from app.services.catalog_metadata_service import background_refresh_catalog_snapshot
from app.services.refresh_service import background_ingestion

_scheduler = None


def start_scheduler():
    global _scheduler

    if _scheduler is not None and _scheduler.running:
        print("Background scheduler already running.")
        return _scheduler

    scheduler = BackgroundScheduler(
        timezone=timezone("Asia/Kolkata"),
        job_defaults={
            "coalesce": True,
            "max_instances": 1,
            "misfire_grace_time": 3600,
        },
    )
    scheduler.add_job(
        background_ingestion,
        id="neo4j_ingestion_refresh",
        trigger="cron",
        hour=0,
        minute=0,
        replace_existing=True,
    )
    scheduler.add_job(
        background_refresh_catalog_snapshot,
        id="catalog_hierarchy_refresh",
        trigger="cron",
        hour=7,
        minute=36,
        replace_existing=True,
    )
    scheduler.start()
    _scheduler = scheduler
    print("Background scheduler started.")
    print("Scheduled Databricks -> Neo4j ingestion at 12:00 AM IST.")
    print("Scheduled catalog_hierarchy.json refresh at 7:32 AM IST.")
    return scheduler
