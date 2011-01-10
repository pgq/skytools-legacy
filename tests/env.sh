
for dir in . .. ../..
do
 config=$dir/config.mak
 test -f $config && break
done

pfx=`grep ^prefix $config | awk '{ print $3}'`
pyver=`python -V 2>&1 | sed 's/Python \([0-9]*.[0-9]*\).*/\1/'`
PYTHONPATH=$pfx/lib/python$pyver/site-packages:$PYTHONPATH
PATH=$pfx/bin:$PATH
#PYTHONPATH=../../python:$PYTHONPATH
#PATH=../../python:../../python/bin:../../scripts:$PATH
#LD_LIBRARY_PATH=/opt/apps/py26/lib:$LD_LIBRARY_PATH
#PATH=/opt/apps/py26/bin:$PATH
export PYTHONPATH PATH LD_LIBRARY_PATH PATH

PGHOST=localhost
export PGHOST


