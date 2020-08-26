# -*- coding: utf-8 -*-
"""
Created on Fri May 15 16:12:47 2020

@author: lijiangman
"""

import threading
from time import sleep
import os
import queue
 
class ExcThread(threading.Thread):
    def __init__(self,group=None, target=None, name=None,
                 args=(), kwargs=None, verbose=None):
        threading.Thread.__init__(self, group, target, name, args, kwargs, verbose)
        if kwargs is None:
            kwargs = {}
        self.__target = target
        self.__args = args
        self.__kwargs = kwargs
        
    def run(self):
        self.exc = None
        try:
            # Possibly throws an exception
            if self.__target:
                self.__target(*self.__args, **self.__kwargs)
        except:
            import sys
            self.exc = sys.exc_info()
        finally:
            # Avoid a refcycle if the thread is running a function with
            # an argument that has a member that points to the thread.
            del self.__target, self.__args, self.__kwargs
    # Save details of the exception thrown but don't rethrow,
    # just complete the function
    
    def join(self):
        threading.Thread.join(self)
        if self.exc:
            msg = "Thread '%s' threw an exception: %s" % (self.getName(), self.exc[1])
            new_exc = Exception(msg)
            temp = self.exc[2]
            self.exc = None
            # raise new_exc.__class__, new_exc, temp
            raise (new_exc.__class__, new_exc, temp)
 
class fileRW(object):
    def __init__(self,fileDir,fileName,file_lock):
        self.__full_file_direction = fileDir+fileName
        self.__fileLock = file_lock
    def clearTime(self):
        fp = open(self.__full_file_direction,'w')
        try:
            fp.write('')
        finally:
            fp.close()
    def writeTimes(self,fileData):
        self.__fileLock.acquire()
        fp = open(self.__full_file_direction,'a+')
        try:
            fp.write(fileData + '\n')
        finally:
            fp.close()
            self.__fileLock.release()
    def readFile(self):
        if os.path.exists(self.__full_file_direction):
            fp = open(self.__full_file_direction)
        else:
            os.system('touch %s' % self.__full_file_direction)
            fp = open(self.__full_file_direction)
        try:
            line = fp.read()
            if None == line:
                fp.close()
                exit
            tmp = line.split()
            file_data = queue.Queue()
            for i in tmp:
                file_data.put(i)
        finally:
            fp.close()
        return file_data
    def readFileMuTh(self):
        global data
        if not data.empty():
            print(data.get()) 
        else:
            raise Exception('read end')
fileDir = './'
fileName = 'file1.txt'
fileLock = threading.Lock()
f = fileRW(fileDir,fileName,fileLock)
def write1():
    for i in range(6):
        f.writeTimes('*_%s' % i)       
def write2():
    for i in range(6):
        f.writeTimes('#_%s' % i)
    
data = f.readFile()
    
if __name__ == '__main__':
    f.clearTime()
    sleep(1)
    th1 = threading.Thread(target=write1)
    th2 = threading.Thread(target=write2)
    th1.start()
    th2.start()
    th1.join()
    th2.join()
    while True:
        try:
            th3 = ExcThread(target=f.readFileMuTh())
            th4 = ExcThread(target=f.readFileMuTh())
            th5 = ExcThread(target=f.readFileMuTh())
            th3.start()
            th4.start()
            th5.start()
            th3.join()
            th4.join()
            th5.join()
            print('#') 
        except Exception as e:
            print(e) 
            break