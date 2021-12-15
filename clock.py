from apscheduler.schedulers.blocking import BlockingScheduler

sched = BlockingScheduler(timezone="America/New_York")

@sched.scheduled_job('cron', day_of_week='mon-fri', hour=21)
def scheduled_job():
    print('This job is run every weekday at 9pm.')

sched.start()