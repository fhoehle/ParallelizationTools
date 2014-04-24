#! /usr/bin/env python
 
import commands,argparse,sys
import Queue
import multiprocessing
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
 	    print "executing ",job
            sys.stdout.flush()
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
    #commandLine parsing
    parser = argparse.ArgumentParser()
    parser.add_argument('--jobFile',required=True,help='file with jobs per line')
    parser.add_argument('--numProcesses',default=3,help="number of processes running parallel")
    parser.add_argument('--debug',default=False,action='store_true',help='debugging')
    args = parser.parse_args()
    numProcesses=int(args.numProcesses)
    jobFile = args.jobFile
    # generate stuff to do
    jobs = []
    with open(jobFile) as f:
     jobs = f.read().splitlines()
    if args.debug:
      print "list to be processed:","\t\n".join(jobs)
    # run
    results = execute(jobs,numProcesses)
 
    # dump results
    for r in results:
        print(r)


