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

    def do_work(self, nums):
        nums = [int(n) for n in nums]
        gevent.sleep(2)
        return str(sum(nums))


if __name__ == '__main__':
  s = zerorpc.Server(Worker())
  ip_port = sys.argv[1]
  ip = ip_port.split(':')[0]
  port = ip_port.split(':')[1]
  s.bind('tcp://'+ip+':'+port)
  c = zerorpc.Client()
  c.connect(master_addr)
  c.register(ip,port)
  s.run()

