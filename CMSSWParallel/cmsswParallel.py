#!/usr/bin/env python
# script for parallel cmssw jobs
import sys,imp,re,argparse,os
###################
class parallelRunner(object):
  def __init__(self,cfgFileName,numProcesses,numJobs,where,debug):
    self.cfgFileName = cfgFileName 
    self.numProcesses = numProcesses 
    self.numJobs = numJobs
    self.where = where
    self.debug = debug
  def createCfgs(self):
    if self.debug:
      print "You launched CMSSW parallel with ",self.numProcesses," processes in parallel and this ",self.cfgFileName," config file, and ",self.numJobs," in total" 
    cfgFile = open(self.cfgFileName,'r')
    cfgFileLoaded = imp.load_source('cfg',self.cfgFileName,cfgFile)
    cfgFile.close()
    process=cfgFileLoaded.process
    ## adapting input modules
    inputFiles=process.source.fileNames.value()
    newNumInputFiles = len(inputFiles)/self.numJobs
    newInputFilesList =  [inputFiles[i:i+newNumInputFiles] for i in range(0, len(inputFiles), newNumInputFiles)]
#    remainingFiles = len(inputFiles) - self.numJobs*newNumInputFiles
#    newNumInputFiles=newNumInputFiles+1
#    newInputFilesList = []
#    for i in range(self.numJobs):
#      newInputFilesList.append(list(inputFiles[i*newNumInputFiles:(i+1)*newNumInputFiles]))
#      if remainingFiles > 0:
#        remainingFiles=remainingFiles-1
#        if remainingFiles == 0:
#          newNumInputFiles=newNumInputFiles-1 
    #output module names
    outputMods = {}
    for outItem in process.outputModules.iteritems():
      outputMods[outItem[0]]=re.match('([^\.]*)(\.[^ \.]*)',outItem[1].fileName.value())
    cfgFileList=[]
    
    for job,inputFs in enumerate(newInputFilesList):
      regexCfgFile = re.match('([^\.]*)(\.[^\.]*)',self.cfgFileName)
      newCfgFileName=regexCfgFile.group(1)+"_"+str(job)+regexCfgFile.group(2)
      #setting source inputFile names
      process.source.fileNames.setValue(inputFs)
      #setting file names for output modules
      for outItem in process.outputModules.iteritems():
        outItem[1].fileName.setValue(outputMods[outItem[0]].group(1)+"_"+str(job)+outputMods[outItem[0]].group(2))
      cfgFileList.append(newCfgFileName)
      f = open(newCfgFileName, 'w')
      f.write(process.dumpPython())
      f.close()
    ## do it parallel
    self.jobFileName=regexCfgFile.group(1)+"_jobs.txt"
    jobFile = open(self.jobFileName,'w')
    for cfg in cfgFileList:
      jobFile.write("cmsRun "+cfg+" >& "+cfg+"_output.txt\n")
    jobFile.close()
  def runParallel(self):
    import commands
    logFile= self.jobFileName.strip()+"_log.txt"
    if self.debug:
      print "using log file",logFile
    statusOutput = commands.getstatusoutput(os.getenv('CMSSW_BASE')+"/ParallelizationTools/BashParallel/doWhatEverParallel.py --jobFile "+self.jobFileName+" --numProcesses "+str(self.numProcesses)+" >& "+logFile)
    if self.debug:
      print "status in total ",statusOutput[0]
    return statusOutput[0]
#########################
if __name__ == "__main__":
  #commandLine parsing
  parser = argparse.ArgumentParser()
  parser.add_argument('--cfgFileName',required=True,help='cfg file which should be executed in parallel')
  parser.add_argument('--numProcesses',default=2,help='number of processes which run parallel')
  parser.add_argument('--numJobs',default=2,help='number of jobs in total')
  parser.add_argument('--where',default='',help='where should it be executed and the output stored')
  parser.add_argument('--usage',action='store_true',default=False,help='help message')
  parser.add_argument('--debug',action='store_true',default=False,help='debugging mode')
  args=parser.parse_args()
  if args.usage:
    parser.print_help()
    sys.exit(0)
  if args.debug:
    print sys.argv
  pR = parallelRunner(args.cfgFileName,args.numProcesses,args.numJobs,args.where,args.debug)
  pR.createCfgs()
  pR.runParallel()
