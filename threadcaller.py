#!/usr/bin/python

from subprocess import call
import sys
import time
import json
from threading import Thread, Semaphore
import threading
from socket import gethostname

def make_combinatory_list(opt, s, l):
    ''' Create a list with all possible parameter combinations
        Inputs:
              opt: list of list of possibilities
                s: string mounted until now (usually called empty)
                l: List to save results.

       This function is recursive.
    '''

    if len(opt) == 0: # should this exist?
        return

    if len(opt) == 1: # Base
        for p in opt[0]:
            l.append(s+" "+str(p))
        return

    else:
        for p in opt[0]:
            make_combinatory_list(opt[1:], s+" "+str(p), l)


class Tasksrc:

    def __init__(self, definition):
        self.ID = definition["ID"]
        self.path_exec = definition["Path_exec"]
        self.parameters = definition["Parameters"]
        self.parameter_names = definition["Param_names"]
        self.of_prefix = definition["Output_file_prefix"]
        self.of_posfix = definition["Output_file_posfix"]
        self.expected_rv = definition["Expected_rv"]  # Unused... For now

        self.src_complete = False

        self.taskInstances = []


    def getTaskID(self):
        return self.ID

    def genTaskInstances(self):

        if len(self.parameter_names) != len(self.parameters):
            raise NameError('Descriptor parameters and names doesnt match\n')

        l = []
        make_combinatory_list(self.parameters, "", l)

        for inst in l:
            s = [ self.path_exec ]
            splitted_inst = inst.split()
            s += splitted_inst  # Nasty

            o = self.of_prefix
            for name, param in zip(self.parameter_names, splitted_inst):
                if name != "":
                    o += ("_" + str(name) + "_" + param)

            o += self.of_posfix
            self.taskInstances.append(TaskInstance(s, o))



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

        return None      # A correct way to do that?!


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
            if (not t.checkCompleteness()) and (not found_task):
                inst = t.getTaskInstance()
                try:
                    inst.start()
                except:
#                    print "SRC FINISHED. Moving ON"
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
