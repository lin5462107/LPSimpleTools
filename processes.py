#encoding=utf-8

import os
import sys
from multiprocessing import Process, Queue

######## task process modules ######## 

class BaseTaskProcess(Process):
    def init(self, *args, **kwargs):
        pass

    def run(self):
        pass

class TaskProcess(BaseTaskProcess):
    def init(self, in_queue=None, out_queue=None):
        if isinstance(in_queue, int):
            self.in_queue = Queue(in_queue)
        else:
            self.in_queue = in_queue
        
        if isinstance(out_queue, int):
            self.out_queue = Queue(out_queue)
        else:
            self.out_queue = out_queue

    def run(self):
        raise NotImplementedError

    def _getData(self):
        return self.in_queue.get()
    
    def _putData(self, data):
        self.out_queue.put(data)

    def putData2Process(self, data):
        self.in_queue.put(data)
    
    def getDataFromProcess(self):
        return self.out_queue.get()

    def cleanProcessQueue(self):
        while self.in_queue.empty() is False:
            self.in_queue.get()

        while self.out_queue.empty() is False:
            self.out_queue.get()

    def outReady(self):
        return self.out_queue.empty() is False

    def setProcessInQueue(self, in_queue):
        self.in_queue = in_queue

    def setProcessOutQueue(self, out_queue):
        self.out_queue = out_queue

######## task pool modules ######## 

class BaseTaskProcessPool(object):
    def init(self, total_proc_inited_objs, in_qsize=1, out_qsize=1, *args, **kwargs):
        self._init(total_proc_inited_objs, in_qtype='none', in_qsize=in_qsize, out_qtype='none', out_qsize=out_qsize) 

    def _init(self, total_proc_inited_objs, in_qtype='none', in_qsize=100, out_qtype='none', out_qsize=100):
        self.total_proc_inited_objs = total_proc_inited_objs

        self.proc_idx_queue = Queue(len(self.total_proc_inited_objs))
        self.process_num = len(self.total_proc_inited_objs)

        self.in_qtype = in_qtype
        self.in_qsize = in_qsize
        self.out_qtype = out_qtype
        self.out_qsize = out_qsize

        self.setPoolInQueues(qtype=in_qtype, qsize=in_qsize)
        self.setPoolOutQueues(qtype=out_qtype, qsize=out_qsize)

    def start(self):
        for proc_obj in self.total_proc_inited_objs:
            proc_obj.start()

    def setPoolInQueues(self, qtype='none', qsize=1):
        self.in_queues = self._getQueues(qtype=qtype, qsize=qsize, num=self.process_num)
        for i, proc_obj in enumerate(self.total_proc_inited_objs):
            proc_obj.setProcessInQueue(queues[i])
    
    def setPoolOutQueues(self, qtype='none', qsize=1):
        self.out_queues = self._getQueues(qtype=qtype, qsize=qsize, num=self.process_num)
        for i, proc_obj in enumerate(self.total_proc_inited_objs):
            proc_obj.setProcessOutQueue(queues[i])

    def _getQueues(self, qtype='none', qsize=1, num=1):
        assert qtype == 'single' or qtype == 'multi' or qtype == 'none'
        assert isinstance(qsize, (int, list, tuple))

        if isinstance(qsize, int):
            qsize = [qsize for _ in range(num)]

        if qtype == 'single':
            q = Queue(qsize[0])
            pool_queues = [q for _ in range(num)]
            return pool_queues

        if qtype == 'multi':
            pool_queues = [Queue(qsize[i]) for i in range(num)]
            return pool_queues

        if qtype == 'none':
            pool_queues = [None for _ in range(num)]
            return pool_queues

class AsyncTaskProcessPool(BaseTaskProcessPool):
    def init(self, total_proc_inited_objs, in_qsize=1, out_qsize=1):
        self._init(total_proc_inited_objs, in_qtype='single', in_qsize=in_qsize, out_qtype='single', out_qsize=out_qsize) 

    def mapDatas2Pool(self, datas):
        for data in datas:
            self.in_queues[0].put(data)

    def mapData2Pool(self, data):
        self.in_queues[0].put(data)

    def getDatasFromPool(self, size=1):
        res = []
        while len(res) < size:
            ret_data = self.out_queues[0].get()
            res.append(ret_data)

        return res

    def getDataFromPool(self):
        return self.out_queues[0].get()

class SyncTaskProcessPool(BaseTaskProcessPool):
    def init(self, total_proc_inited_objs, in_qsize=1, out_qsize=1):
        self._init(total_proc_inited_objs, in_qtype='multi', in_qsize=in_qsize, out_qtype='multi', out_qsize=out_qsize)

    def mapDatas2Pool(self, datas):
        doing_procs = []
        done_num = 0
        not_map_datas = [[i, datas[i]] for i in range(len(datas))]
        res = [None for _ in range(len(datas))]
        
        while len(not_map_datas) != 0 or len(doing_procs) != 0:
            while len(not_map_datas) != 0 and self.proc_idx_queue.empty() is False:
                proc_idx = self.proc_idx_queue.get()
                proc = self.total_proc_inited_objs[proc_idx]

                i, data = not_map_datas.pop()

                proc.cleanProcessQueue()
                proc.putData2Process(data)
                doing_procs.append([i, proc_idx])

            new_doing_procs = []
            for idx, proc_idx in doing_procs:
                proc = self.total_proc_inited_objs[proc_idx]
                if proc.outReady():
                    res[idx] = proc.getDataFromProcess()
                    self.proc_idx_queue.put(proc_idx)
                else:
                    new_doing_procs.append([idx, proc_idx])
            doing_procs = new_doing_procs

        return res

    def mapData2Pool(self, data):
        proc_idx = self.proc_idx_queue.get()
        proc = self.total_proc_inited_objs[proc_idx]
        proc.cleanProcessQueue()
        proc.putData2Process(data)
        res = proc.getDataFromProcess()
        self.proc_idx_queue.put(proc_idx)
        return res

# ######## channel modules ######## 

# class BaseProcessChannel(object):
#     def init(self, total_pool_inited_objs, channel_qtype, channel_qsize, *args, **kwargs):
#         pass

#     def _init(self, total_pool_inited_objs, channel_type):
        
        

#     def start(self):
#         for pool_obj in self.total_pool_inited_objs:
#             pool_obj.start()
