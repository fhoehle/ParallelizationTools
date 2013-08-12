#!/usr/bin/env python
# script for parallel cmssw jobs
import getopt,sys,imp,re
#commandLine parsing
myArgs=['cfgFileName=','numProcesses=','numJobs=']
opts, args = getopt.getopt(sys.argv[1:], '',myArgs)
print sys.argv
cfgFileName=None
numProcesses=2
numJobs=2
for opt,arg in opts:
 if opt in ("--cfgFileName"):
  cfgFileName=str(arg)
  sys.argv.remove('--cfgFileName')
  sys.argv.remove(arg)
 if opt in ("--numProcesses"):
  sys.argv.remove('--numProcesses')
  numProcesses=int(arg)
  sys.argv.remove(arg)
 if opt in ("--numJobs"):
  numJobs=int(arg)
  sys.argv.remove('--numJobs')
  sys.argv.remove(str(arg))
print sys.argv
print "You launched CMSSW parallel with ",numProcesses," processes in parallel and this ",cfgFileName," config file, and ",numJobs," in total" 
cfgFile = open(cfgFileName,'r')
cfgFileLoaded = imp.load_source('cfg',cfgFileName,cfgFile)
cfgFile.close()
process=cfgFileLoaded.process
inputFiles=process.source.fileNames.value()
newNumInputFiles = len(inputFiles)/numJobs
remainingFiles = len(inputFiles) - numJobs*newNumInputFiles
newNumInputFiles=newNumInputFiles+1
newInputFilesList = []
for i in range(numJobs):
 newInputFilesList.append(list(inputFiles[i*newNumInputFiles:(i+1)*newNumInputFiles]))
 if remainingFiles > 0:
  remainingFiles=remainingFiles-1
  if remainingFiles == 0:
   newNumInputFiles=newNumInputFiles-1 
#output module names
outputMods = {}
for outItem in process.outputModules.iteritems():
 outputMods[outItem[0]]=re.match('([^\.]*)(\.[^ \.]*)',outItem[1].fileName.value())
cfgFileList=[]

for job in range(numJobs):
 regexCfgFile = re.match('([^\.]*)(\.[^\.]*)',cfgFileName)
 newCfgFileName=regexCfgFile.group(1)+"_"+str(job)+regexCfgFile.group(2)
 #setting source inputFile names
 process.source.fileNames.setValue(newInputFilesList[job])
 #setting file names for output modules
 for outItem in process.outputModules.iteritems():
  outItem[1].fileName.setValue(outputMods[outItem[0]].group(1)+"_"+str(job)+outputMods[outItem[0]].group(2))
 cfgFileList.append(newCfgFileName)
 f = open(newCfgFileName, 'w')
 f.write(process.dumpPython())
 f.close()
## do it parallel
jobFileName=regexCfgFile.group(1)+"_jobs.txt"
jobFile = open(jobFileName,'w')
for cfg in cfgFileList:
 jobFile.write("cmsRun "+cfg+" >& "+cfg+"_output.txt\n")
jobFile.close()
import commands
statusOutput = commands.getstatusoutput("./doWhatEverParallel.py --jobFile "+jobFileName+" --numProcesses "+str(numProcesses)+" >& "+regexCfgFile.group(1)+"_jobs_parallel.txt")
print "status in total ",statusOutput[0]
