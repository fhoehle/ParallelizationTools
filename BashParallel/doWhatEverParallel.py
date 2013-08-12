#! /usr/bin/env python
 
import commands,getopt,sys
import Queue
import multiprocessing
#commandLine parsing
opts, args = getopt.getopt(sys.argv[1:], '',['jobFile=','numProcesses='])
jobFile=None
numProcesses=3
for opt,arg in opts:
 #print opt , " :   " , arg
 if opt in  ("--jobFile"):
  jobFile=arg
 if opt in  ("--numProcesses"):
  numProcesses=int(arg)

class Worker(multiprocessing.Process):
 
    def __init__(self,
            work_queue,
            result_queue):
 
        # base class initialization
        multiprocessing.Process.__init__(self)
 
        # job management stuff
        self.work_queue = work_queue
        self.result_queue = result_queue
        self.kill_received = False
 
    def run(self):
        while not self.kill_received:
 
            # get a task
            try:
                job = self.work_queue.get_nowait()
            except Queue.Empty:
                break
 	    print "it me the worker with job ",job
            # the actual processing
            statusOutput = commands.getstatusoutput(job)
 	    print "done with job ",job
            # store the result
            self.result_queue.put(statusOutput)
 
def execute(jobs, num_processes=2):
 
    # load up work queue
    work_queue = multiprocessing.Queue()
    for job in jobs:
        work_queue.put(job)
        print "job in workerqueue ",job
    # create a queue to pass to workers to store the results
    result_queue = multiprocessing.Queue()
 
    # spawn workers
    print "this many workers ",range(num_processes)
    for i in range(num_processes):
	print "creating worker ",i
        worker = Worker(work_queue, result_queue)
	print "starting worker",i
        worker.start()
	print "worker started",i
 
    # collect the results off the queue
    results = []
    while len(results) < len(jobs):
        result = result_queue.get()
        results.append(result)
 
    return results
 
if __name__ == "__main__":
 
    # generate stuff to do
    jobs = []
    file = open(jobFile)
    jobs = file.readlines()
    with open(jobFile) as f:
     jobs = f.read().splitlines()

    print jobs 
    # run
    results = execute(jobs,numProcesses)
 
    # dump results
    for r in results:
        print(r)


