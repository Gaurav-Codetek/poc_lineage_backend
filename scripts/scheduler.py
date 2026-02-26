import time

from app.jobs.scheduler import start_scheduler


def run():
    start_scheduler()
    while True:
        time.sleep(60)


if __name__ == "__main__":
    run()
