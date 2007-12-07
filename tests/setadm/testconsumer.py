#! /usr/bin/env python

import sys, pgq

class TestConsumer(pgq.SetConsumer):
    pass

if __name__ == '__main__':
    script = TestConsumer('test_consumer', sys.argv[1:])
    script.start()

