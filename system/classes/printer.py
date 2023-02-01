# coding=utf-8

from random import randint
import sys, time, datetime, os

class Printer:
    def __init__(self, instance_path, DEBUG=False):
        #os.makedirs(os.path.dirname(instance_path), exist_ok=True)
        #self.logger = open(instance_path, 'a', encoding='utf-8')
        self.DEBUG = DEBUG

    def print_update(self, message):
        #self.logger.write("["+datetime.datetime.fromtimestamp(time.time()).strftime('%Y-%m-%d %H:%M:%S')+"] "+message+"\n")
        #self.logger.flush()
        if self.DEBUG:
            print(message)

    def counter(self, startRange, endRange):
        s = randint(startRange, endRange)
        #self.logger.write("Sleep for "+str(s)+" seconds...\n")
        for _ in range(s, 0, -1):
            time.sleep(1)