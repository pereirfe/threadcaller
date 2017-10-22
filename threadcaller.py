#!/usr/bin/python

from subprocess import call
import sys
import time
import json
from threading import Thread, Semaphore
import threading
from socket import gethostname

class Tasksrc:

    def __init__(self, definition):
        self.ID = definition["ID"]
        self.repetitions = definition["Repetitions"]
        if  definition["Prereq_ID"] == "None":
            self.preID = None
        else:
            self.preID = definition["Prereq_ID"]

        self.path_exec = definition["Path_exec"]
        self.param_prefix = definition["Param_prefix"]
        self.var = definition["Param_var"]
        self.param_posfix = definition["Param_posfix"]
        self.of_prefix = definition["Output_file_prefix"]
        self.of_posfix = definition["Output_file_posfix"]
        self.expected_rv = definition["Expected_rv"]  # Unused... For now

        self.src_complete = False
        self.ready = False
        if self.preID == None:
            self.ready = True

        self.taskInstances = []


    def getTaskID(self):
        return self.ID

    def genTaskInstances(self):

        pref = self.param_prefix.split()
        posf = self.param_posfix.split()

        for v in self.var:
            for i in range(self.repetitions):
                s = [ self.path_exec ]
                s += pref
                s += [str(v)]
                s += posf
                o = self.of_prefix + "_" + str(v) + "_IT" + str(i) + "_" + self.of_posfix
                self.taskInstances.append(TaskInstance(s, o))


    def checkReadyness(self, allsrc):
        self.ready = True
        for src in allsrc:
            if src.ID == self.preID and src.src_complete == False:
                self.ready == False
        return self.ready


    def checkCompleteness(self):
        self.src_complete = True
        for tki in self.taskInstances:
            if not tki.inst_complete:
                self.src_complete = False

        return self.src_complete

    def getTaskInstance(self):
        for tki in self.taskInstances:
            if not tki.inst_complete and not tki.running:
                return tki

        print "ERROR___EMPTY"
        return "NO THREAD"      # A correct way to do that?!


class TaskInstance(Tasksrc):
    def __init__(self, callv, output_file):
        self.callv = callv
        self.running = False
        self.inst_complete = False
        self.output_file = output_file

    def getCallS(self):
        s = ""
        for x in self.callv:
            s += x
            s += " "
        return s

    def getCallV(self):
        return self.callv

    def getOutputFile(self):
        return self.output_file

    def finish(self):
        self.inst_complete = True
        self.running = False

    def start(self):
        self.running = True


def runTask(tki, semaphore):
    semaphore.acquire()
    f = open(tki.getOutputFile(), "w")
    call(tki.getCallV(), stdout=f, stderr=f)
    tki.finish()
    f.close()
    semaphore.release()


def threadcaller(numproc, descriptor, report):
    tv = []
    threads = []
    rp = open(report, "w")
    host = gethostname()
    print time.strftime("%a, %d %b %Y %H:%M:%S:\t", time.localtime()), "@"+host+"\t",
    print "Starting Descriptor Loading..."
    print >>rp, time.strftime("%a, %d %b %Y %H:%M:%S:\t", time.localtime()), "@"+host+"\t",
    print >>rp, "Starting Descriptor Loading..."
    with open(descriptor, "r") as infile:
        v = json.load(infile)

        for t in v:
            src = Tasksrc(t)
            src.genTaskInstances()
            tv.append(src)

    print time.strftime("%a, %d %b %Y %H:%M:%S:\t", time.localtime()), "@"+host+"\t",
    print "Loaded"
    print >>rp, time.strftime("%a, %d %b %Y %H:%M:%S:\t", time.localtime()), "@"+host+"\t",
    print >>rp, "Loaded"
    
    sem = Semaphore(int(numproc))

    all_finished = False

    while all_finished == False:
        found_task = False
        for t in tv:
            if t.checkReadyness(tv) and (not t.checkCompleteness()) and (not found_task):
                inst = t.getTaskInstance()
                try:
                    inst.start()
                except:
                    print "SRC FINISHED. Moving ON"
                    continue

                print time.strftime("%a, %d %b %Y %H:%M:%S:\t", time.localtime()), "@"+host+"\t",
                print "Starting instance from ID:", t.getTaskID(), "cmd", inst.getCallS()
                print >>rp, time.strftime("%a, %d %b %Y %H:%M:%S:\t", time.localtime()), "@"+host+"\t",
                print >>rp, "Starting instance from ID:", t.getTaskID(), "cmd", inst.getCallS()
                rp.flush()
                threadinst = Thread(target = runTask, args = (inst, sem))
                threadinst.start()
                found_task = True
                break

        # This pause is necessary to avoid multiple prints
        # while waiting last processes to run
        # (TODO: Find a better way to do that)
        time.sleep(0.2)
        sem.acquire()           # Block until a task finishes
        sem.release()

        all_finished = True
        for t in tv:
            if not t.checkCompleteness():
                all_finished = False

        if all_finished:
            print time.strftime("%a, %d %b %Y %H:%M:%S:\t", time.localtime()), "@"+host+"\t",
            print "All Tasks finished", t.getTaskID()
            print >>rp, time.strftime("%a, %d %b %Y %H:%M:%S:\t", time.localtime()), "@"+host+"\t",
            print >>rp, "All Tasks finished", t.getTaskID()
            rp.close()
            break

if __name__ == '__main__':
    if len(sys.argv) != 4:
        print "Usage:", sys.argv[0], "numproc descriptor.json reportfile"
    else:
        threadcaller(int(sys.argv[1]), sys.argv[2], sys.argv[3])
