#! /bin/sh

bdir=`echo build/lib.*`
#cd python
#export PYTHONPATH=.:../$bdir:$PYTHONPATH
#echo $PYTHONPATH

cd $bdir
export PYTHONPATH=.:$PYTHONPATH
pylint -i yes --rcfile=../../misc/lint.rc skytools

