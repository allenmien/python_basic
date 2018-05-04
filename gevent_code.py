import gevent
from gevent.queue import Queue

tasks = Queue(maxsize=2)
Thread_SIZE = 3


def worker():
    while True:
        try:
            task = tasks.get()
            if task == StopIteration:
                break
            print('Worker got task %s' % (task))
        except Exception as e:
            print u"error : " + str(e)
    print('Quitting time!')


def boss():
    for i in range(1, 20):
        tasks.put(i)
    for i in range(Thread_SIZE):
        tasks.put(StopIteration)


task_list = list()
task_list.append(gevent.spawn(boss))
for i in range(Thread_SIZE):
    task_list.append(gevent.spawn(worker))

gevent.joinall(task_list)
