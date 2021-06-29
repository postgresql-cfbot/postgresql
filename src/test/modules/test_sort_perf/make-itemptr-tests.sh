#!/bin/sh

# different values to test for insertion sorts
THRESHOLDS="7 8 9 10 11 12 13"

# amount of data to sort, in megabytes
MEMORY=128

# make 32 bit and 64 bit sort functions for each insertion threshold
for threshold in $THRESHOLDS ; do
  echo "#define ST_SORT sort_${threshold}_itemptr"
  echo "#define ST_ELEMENT_TYPE ItemPointerData"
  echo "#define ST_COMPARE(a, b) itemptr_comparator(a, b)"
  echo "#define ST_SCOPE static"
  echo "#define ST_SORT_SMALL_THRESHOLD $threshold"
  echo "#define ST_CHECK_FOR_INTERRUPTS"
  echo "#define ST_DECLARE"
  echo "#define ST_DEFINE"
  echo "#include \"lib/sort_template.h\""
  echo
  echo "#define ST_SORT sort64_${threshold}_itemptr"
  echo "#define ST_ELEMENT_TYPE ItemPointerData"
  echo "#define ST_COMPARE(a, b) (itemptr_encode(a) - itemptr_encode(b))"
  echo "#define ST_COMPARE_RET_TYPE int64"
  echo "#define ST_SCOPE static"
  echo "#define ST_SORT_SMALL_THRESHOLD $threshold"
  echo "#define ST_CHECK_FOR_INTERRUPTS"
  echo "#define ST_DECLARE"
  echo "#define ST_DEFINE"
  echo "#include \"lib/sort_template.h\""
  echo
done

# generate the function that runs all the tests
echo "static void"
echo "do_sort_itemptr(void)"
echo "{"
echo "    size_t nobjects = (1024 * 1024 * (size_t) $MEMORY) / sizeof(ItemPointerData);"
echo "    ItemPointerData *unsorted = malloc(sizeof(ItemPointerData) * nobjects);"
echo "    ItemPointerData *sorted = malloc(sizeof(ItemPointerData) * nobjects);"
echo

for order in random increasing decreasing ; do
  echo "    for (size_t i = 0; i < nobjects; ++i)"
  echo "    {"
  if [ "$order" = "random" ] ; then
    echo "      long v = random();"
  elif [ "$order" = "increasing" ] ; then
    echo "      long v = i + 16;"
  elif [ "$order" = "decreasing" ] ; then
    echo "      long v = INT_MAX - i;"
  fi
  echo "        ItemPointerSet(&unsorted[i], (BlockNumber) (v >> 4) | 1, (v & 0xf) + 1);"
  echo "    }"
  echo

  for threshold in $THRESHOLDS ; do
    if [ "$threshold" = 7 ] ; then
      # compare threshold 7 against non-specialized qsort too
      echo "    for (int i = 0; i < 3; ++i)"
      echo "    {"
      echo "        instr_time start_time, end_time;"
      echo "        memcpy(sorted, unsorted, sizeof(sorted[0]) * nobjects);"
      echo "        INSTR_TIME_SET_CURRENT(start_time);"
      echo "        qsort(sorted, nobjects, sizeof(ItemPointerData), itemptr_comparator);"
      echo "        INSTR_TIME_SET_CURRENT(end_time);"
      echo "        INSTR_TIME_SUBTRACT(end_time, start_time);"
      echo "        elog(NOTICE, \"[traditional qsort] order=$order, threshold=$threshold, cmp=32, test=%d, time=%f\", i, INSTR_TIME_GET_DOUBLE(end_time));"
      echo "    }"
    fi
    echo "   for (int i = 0; i < 3; ++i)"
    echo "    {"
    echo "        instr_time start_time, end_time;"
    echo "        memcpy(sorted, unsorted, sizeof(sorted[0]) * nobjects);"
    echo "        INSTR_TIME_SET_CURRENT(start_time);"
    echo "        sort_${threshold}_itemptr(sorted, nobjects);"
    echo "        INSTR_TIME_SET_CURRENT(end_time);"
    echo "        INSTR_TIME_SUBTRACT(end_time, start_time);"
    echo "        elog(NOTICE, \"order=$order, threshold=$threshold, cmp=32, test=%d, time=%f\", i, INSTR_TIME_GET_DOUBLE(end_time));"
    echo "    }"
    echo "    for (int i = 0; i < 3; ++i)"
    echo "    {"
    echo "        instr_time start_time, end_time;"
    echo "        memcpy(sorted, unsorted, sizeof(sorted[0]) * nobjects);"
    echo "        INSTR_TIME_SET_CURRENT(start_time);"
    echo "        memcpy(sorted, unsorted, sizeof(sorted[0]) * nobjects);"
    echo "        sort64_${threshold}_itemptr(sorted, nobjects);"
    echo "        INSTR_TIME_SET_CURRENT(end_time);"
    echo "        INSTR_TIME_SUBTRACT(end_time, start_time);"
    echo "        elog(NOTICE, \"order=$order, threshold=$threshold, cmp=64, test=%d, time=%f\", i, INSTR_TIME_GET_DOUBLE(end_time));"
    echo "    }"
  done
done

echo "    free(sorted);"
echo "    free(unsorted);"
echo "}"
