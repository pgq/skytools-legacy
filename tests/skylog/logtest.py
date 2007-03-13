#! /usr/bin/env python

import sys, os, skytools

import skytools.skylog

class LogTest(skytools.DBScript):
    def work(self):
        self.log.error('test error')
        self.log.warning('test warning')
        self.log.info('test info')
        self.log.debug('test debug')

if __name__ == '__main__':
    script = LogTest('log_test', sys.argv[1:])
    script.start()

