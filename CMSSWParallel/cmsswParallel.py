#!/usr/bin/env python
# script for parallel cmssw jobs
import sys,imp,re,argparse
#commandLine parsing
parser = argparse.ArgumentParser()
parser.add_argument('--cfgFileName',required=True,help='cfg file which should be executed in parallel')
parser.add_argument('--numProcesses',default=2,help='number of processes which run parallel')
parser.add_argument('--numJobs',default=2,help='number of jobs in total')
parser.add_argument('--where',default='',help='where should it be executed and the output stored')
parser.add_argument('--usage',action='store_true',default=False,help='help message')
args=parser.parse_args()
if args.usage:
  parser.print_help()
  sys.exit(0)
print sys.argv
cfgFileName=args.cfgFileName
numProcesses=args.numProcesses
numJobs=args.numJobs
where = args.where
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
