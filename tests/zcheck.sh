#! /bin/sh

grep -E 'ERR|WARN|CRIT' log/*.log || echo "All OK"

