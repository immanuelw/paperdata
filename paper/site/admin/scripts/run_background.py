'''
paper.site.admin.scripts.run_background

runs background pull script

author | Immanuel Washington
'''
import os
from apscheduler.schedulers.blocking import BlockingScheduler

scheduler = BlockingScheduler()

@scheduler.scheduled_job('interval', hours=4)
def pull_chart_data():
    print('pull_chart_data')
    os.system('python -m scripts.pull_chart_data')

if __name__ == '__main__':
    scheduler.start()
