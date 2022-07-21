# ../papers-sorting/sort-bench-pdqsort-sql-20220529.sh 1000 setup

set -e

#ROWS=10000000
#../papers-sorting/sort-bench-pdqsort-sql-20220529.sh $ROWS setup


# test actual funcs
for threshold in 7 10 12 14 16 18 20 22 24 32; do
#for threshold in 7 10; do

# version 10
echo "Threshold: $threshold"

# TODO: run queries
#../rebuild.sh
#../papers-sorting/sort-bench-pdqsort-sql-20220529.sh $ROWS
#mv results.csv queryresult-$(date +'%Y%m%d')-$threshold.csv

# run microbenchmark

# build perf module
pushd src/test/modules/test_sort_perf/ >/dev/null
touch test_sort_perf.c
make -s CPPFLAGS=-DST_INSERTION_SORT_THRESHOLD=$threshold && make install -s
popd >/dev/null

padthreshold=$(printf "%02d" $threshold)
./inst/bin/psql -c 'drop extension if exists test_sort_perf; create extension test_sort_perf;'
./inst/bin/psql -c 'select test_sort_cmp_weight(1 * 1024*1024)' 2> microresult-$padthreshold.txt
#./inst/bin/psql -c 'select test_sort_cmp_weight(1 * 1024)' 2> microresult-$padthreshold.txt

perl -n -e 'print "$1\t$2\t$3\t$4\t$5\n" if /NOTICE:  \[(.+)\] num=(\d+), threshold=(\d+), order=(\w+), time=(\d+\.\d+)/;' microresult-$padthreshold.txt > microresult-$(date +'%Y%m%d')-$padthreshold.csv

done

