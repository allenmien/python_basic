# -*-coding:utf-8-*-
import time

import datetime
from gevent.pool import Pool
import gevent.monkey

gevent.monkey.patch_all()
pool = Pool(3)


def boss(j):
    print u"size : " + str(len(pool))
    time.sleep(1)
    print j


start_time = datetime.datetime.now()
for i in range(10):
    pool.spawn(boss, i)
pool.join()
# pool.map(boss, range(10))
print u"finished"

time_delta = datetime.datetime.now() - start_time
print time_delta
