import sys

import zerorpc
import gevent
import mapreduce
import itertools


#master_addr = 'tcp://0.0.0.0:4242'
master_addr = 'tcp://10.0.0.25:4242'

class Master(object):

    def __init__(self):
        gevent.spawn(self.controller)
        self.state = 'READY'
        self.workers = {}

    def controller(self):
        while True:
            print '[Master:%s] ' % (self.state),
            for w in self.workers:
                print '(%s,%s,%s,%s)' % (w[0], w[1], self.workers[w][0],self.workers[w][1]),
            print
            for w in self.workers:
                self.workers[w][1].ping()

            gevent.sleep(1)

    def register_async(self, ip, port):
        print '[Master:%s] ' % self.state,
        print 'Registered worker (%s,%s)' % (ip, port)
        c = zerorpc.Client()
        c.connect("tcp://" + ip + ':' + port)
        self.workers[(ip,port)] = ('READY', c)
        c.ping()

    def register(self, ip, port):
        gevent.spawn(self.register_async, ip, port)
  
   #ffile is the name of file to input
    def do_job(self, ffile):
        n = len(self.workers)
        f = open(ffile)
        values = f.readlines()
        # every worker works on one chunk
        chunk = len(values) / n
        i = 0 # index of worker
        offset = 0 # position of file that has been arranged for worker. 
        #result = 0
        procs = []
        for w in self.workers:
            print 'there are '+str(n)+' workers in all' 
            if i == (n - 1):
                print 'last worker'
                sub = values[offset:]
            else:
                sub = values[offset:offset+chunk]
            proc = gevent.spawn(self.workers[w][1].do_work, sub)
            procs.append(proc)

            #result += int(self.workers[w][1].do_work(sub))
            i = i + 1
            offset = offset + chunk

        gevent.joinall(procs)
        #return sum([int(p.value) for p in procs])
        #return list(itertools.chain(*procs)) 
        res = [] 
        # Combine result: sum value of same key. 
        for l in procs:
          res.append(l.value)
        return res
class Worker(object):
    def __init__(self):
        gevent.spawn(self.controller)
        pass

    def controller(self):
        while True:
            print('[Worker]')
            gevent.sleep(1)

    def ping(self):
        print('[Worker] Ping from Master')

    def do_work(self, sub):
        engine = mapreduce.Engine(sub, WordCountMap, WordCountReduce)
        engine.execute()
        result_list = engine.get_result_list()
        
        gevent.sleep(2)
        return result_list 
        #return str(sum(nums))

class WordCountMap(mapreduce.Map):

    def map(self, k, v):
        words = v.split()
        for w in words:
            self.emit(w, '1')

class WordCountReduce(mapreduce.Reduce):

    def reduce(self, k, vlist):
        count = 0
        for v in vlist:
            count = count + int(v)
        self.emit(k + ':' + str(count))

if __name__ == '__main__':

    cmd = sys.argv[1]

    if cmd == 'master':
        s = zerorpc.Server(Master())
        s.bind(master_addr)
        s.run()
    elif cmd == 'worker':
        s = zerorpc.Server(Worker())
        ip = '0.0.0.0'
        port = sys.argv[2]
        s.bind('tcp://' + ip + ':' + port)
        c = zerorpc.Client()
        c.connect(master_addr)
        c.register(ip, port)
        s.run()
    elif cmd == 'client':
        c = zerorpc.Client()
        c.connect(master_addr)
        # read in file. (then word count) 
        filename = sys.argv[2] 
        #result = c.do_job('\'' + filename + '\'')
        result = c.do_job(filename)
        print result






