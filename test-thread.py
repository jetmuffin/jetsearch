# import time
# import thread
#
# def heartbeat():
#     while True:
#         print "heartbeat"
#         time.sleep(3)
#
# def work():
#     cnt = 0
#     while cnt < 10:
#         print cnt
#         cnt += 1
#         time.sleep(1)
#     thread.exit_thread()
#
# thread.start_new_thread(heartbeat, ())
# thread.start_new_thread(work, ())
import threading

import time

mylock = threading.RLock()
num=1

class myThread(threading.Thread):
    def __init__(self, name):
        threading.Thread.__init__(self)
        self.t_name = name

    def run(self):
        global num
        while True:
            mylock.acquire()
            print '\nThread(%s) locked, Number: %d'%(self.t_name, num)
            if num>=10:
                mylock.release()
                print '\nThread(%s) released, Number: %d'%(self.t_name, num)
                break
            num+=1
            print '\nThread(%s) released, Number: %d'%(self.t_name, num)
            mylock.release()
            time.sleep(5)
def heartbeat():
    global num
    while True:
        mylock.acquire()
        print '\nNumber: %d'%(num)
        if num>=10:
            mylock.release()
            print '\n Number: %d'%(num)
            break
        num+=1
        print '\n %d'%( num)
        mylock.release()
        time.sleep(5)
def test():
    try:
        thread2 = threading.Thread(target=heartbeat)
        thread2.start()
        for i in xrange(10):
            print "123"
            time.sleep(3)
    except KeyboardInterrupt:
        thread2.join()

if __name__== '__main__':
    test()