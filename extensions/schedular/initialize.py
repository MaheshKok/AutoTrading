from flask_apscheduler import APScheduler


def register_scheduler(app):
    # initialize scheduler
    scheduler = APScheduler()
    # if you don't wanna use a config, you can set options here:
    scheduler.api_enabled = True
    scheduler.init_app(app)
    # scheduler.start()
    return scheduler

