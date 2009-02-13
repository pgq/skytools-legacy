#! /bin/sh

PYTHONPATH=python:$PYTHONPATH
export PYTHONPATH


for f in \
python/skytools/*.py \
python/pgq/*.py \
python/pgq/cascade/*.py \
python/londiste/*.py \
python/*.py \
scripts/*.py
do
  pychecker --config misc/pychecker.rc "$f"
done

