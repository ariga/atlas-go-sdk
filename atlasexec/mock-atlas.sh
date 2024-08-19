#!/bin/bash

if [[ "$TEST_ARGS" != "$@" ]]; then
  >&2 echo "Receive unexpected args: $@"
  exit 1
fi

if [[ "$TEST_STDOUT" != "" ]]; then
  echo $TEST_STDOUT
  exit 0
fi

TEST_STDERR="${TEST_STDERR:-Missing stderr either stdout input for the test}"
echo $TEST_STDERR
exit 1
