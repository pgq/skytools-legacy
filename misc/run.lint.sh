#! /bin/sh

bdir=`echo build/lib.*`
#cd python
#export PYTHONPATH=.:../$bdir:$PYTHONPATH
#echo $PYTHONPATH

#cd $bdir
cd python
export PYTHONPATH=.:$PYTHONPATH
pylint -i yes --rcfile=../misc/lint.rc -E skytools pgq londiste

