from multiprocessing import Process
import time

class ProcessPool:
    def __init__(self,num):
        self.num=num
        self._p_list = []
    def spawn(self,func,*args):
        while True:
            self._p_list = list(filter(lambda x:x.is_alive(),self._p_list))
            if len(self._p_list) < self.num:
                self._run(func,*args)
                break
            time.sleep(0.005)
    def _run(self,func,*args):
        p = Process(target=func,args=args)
        p.start()
        self._p_list.append(p)