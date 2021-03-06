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
    logDict = {}
    inputFiles=process.source.fileNames.value()
    newNumInputFiles = max([len(inputFiles)/self.numJobs , 1])
    newInputFilesList =  [inputFiles[i:i+newNumInputFiles] for i in range(0, len(inputFiles), newNumInputFiles)]
    outputMods = {}
    for outItem in process.outputModules.iteritems():
      outputMods[outItem[0]]=re.match('([^\.]*)(\.[^ \.]*)',outItem[1].fileName.value())
    cfgFileList=[]
    
    for job,inputFs in enumerate(newInputFilesList):
      jobKey = 'job_'+str(job)
      logDict[jobKey] = {'num':job}
      regexCfgFile = re.match('([^\.]*)(\.[^\.]*)',self.cfgFileName)
      newCfgFileName=regexCfgFile.group(1)+"_"+str(job)+regexCfgFile.group(2)
      logDict[jobKey]['cfg']=newCfgFileName;
      #setting source inputFile names
      process.source.fileNames.setValue(inputFs)
      logDict[jobKey]['inputFiles']=inputFs
      #setting file names for output modules
      logDict[jobKey]['output'] = {}
      for outItem in process.outputModules.iteritems():
        newOutputName = outputMods[outItem[0]].group(1)+"_"+str(job)+outputMods[outItem[0]].group(2)
        outItem[1].fileName.setValue(newOutputName)
        logDict[jobKey]['output'][outItem[0]] = newOutputName
      cfgFileList.append(newCfgFileName)
      f = open(newCfgFileName, 'w')
      f.write(process.dumpPython())
      f.close()
      self.logDict = logDict
    ## do it parallel
    self.jobFileName=regexCfgFile.group(1)+"_jobs.txt"
    jobFile = open(self.jobFileName,'w')
    cmds = ["cmsRun "+cfg+" >& "+cfg+"_output.txt" for cfg in cfgFileList]
    for c in cmds:
      jobFile.write(c+"\n")
    jobFile.close()
    return cmds
  def runParallel(self):
    import commands
    import subprocess
    logFile= self.jobFileName.strip()+"_log.txt"
    if self.debug:
      print "using log file",logFile
    stopKey = 'stopKeyDONE'
    command = os.getenv('CMSSW_BASE')+"/ParallelizationTools/BashParallel/doWhatEverParallel.py --jobFile "+self.jobFileName+" --numProcesses "+str(self.numProcesses)+" >& "+logFile+' ;echo "returnCodeCrab: "$?"!"; echo "'+stopKey+'"'
    subPStdOut = [];exitCode=None
    subPr = subprocess.Popen([command],bufsize=1 , stdin=open(os.devnull),shell=True,stdout=(open(self.stdoutTMPfile,'w') if hasattr(self,'stdoutTMPfile') and self.stdoutTMPfile else subprocess.PIPE ),env=os.environ)
    #statusOutput = commands.getstatusoutput(os.getenv('CMSSW_BASE')+"/ParallelizationTools/BashParallel/doWhatEverParallel.py --jobFile "+self.jobFileName+" --numProcesses "+str(self.numProcesses)+" >& "+logFile)
    import re
    crabExitCode = None
    for i,line in enumerate(iter(  subPr.stdout.readline ,stopKey+'\n')):
      if 'returnCodeCrab' in line:
        reExitcode = re.match('^.*returnCodeCrab:\ ([0-9][0-9]*)\ *.*$',line)
        if reExitcode and len(reExitcode.groups()) > 0:
          crabExitCode = reExitcode.group(1)
        else:
          print "bad exitCode:\n  ","  \n".join(subPStdOut[-6:])
      subPStdOut.append(line)
    self.logDict['exitCode']=crabExitCode
    import json
    self.jsonLogFileName = self.cfgFileName.strip()+'_runParallellog_JSON.txt'
    print  self.jsonLogFileName
    with open(self.jsonLogFileName,'w') as jsonLog:
      json.dump(self.logDict,jsonLog)
    return crabExitCode #" ".join(subPStdOut)
#########################
if __name__ == "__main__":
  #commandLine parsing
  parser = argparse.ArgumentParser()
  parser.add_argument('--cfgFileName',required=True,help='cfg file which should be executed in parallel')
  parser.add_argument('--numProcesses',default=2,type=int,help='number of processes which run parallel')
  parser.add_argument('--numJobs',default=2,type=int,help='number of jobs in total')
  parser.add_argument('--where',default='',help='where should it be executed and the output stored')
  parser.add_argument('--usage',action='store_true',default=False,help='help message')
  parser.add_argument('--debug',action='store_true',default=False,help='debugging mode')
  args=parser.parse_args()
  if args.usage:
    parser.print_help()
    sys.exit(0)
  if args.debug:
    print sys.argv
  pR = parallelRunner(args.cfgFileName,int(args.numProcesses),int(args.numJobs),args.where,args.debug)
  pR.createCfgs()
  pR.runParallel()
