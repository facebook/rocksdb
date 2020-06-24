#! /bin/bash

# Generating these .cc files simplifies some logic in Makefile, because all
# source files are .cc (or .c)

for CPP_FILE in `find folly -name '*.cpp'`
do
  echo "#include \"$(basename $CPP_FILE)\"" > `echo $CPP_FILE | sed s/cpp$/cc/`
done
