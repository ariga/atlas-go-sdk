#!/bin/bash

if [[ "$TEST_ARGS" != "$@" ]]; then
  >&2 echo "Receive unexpected args: $@"
  exit 1
fi

if [[ "$TEST_STDOUT" != "" ]]; then
  echo -n $TEST_STDOUT
  if [[ "$TEST_STDERR" == "" ]]; then
    exit 0 # No stderr
  fi
  # In some cases, Atlas will write the error in stderr
  # when if the command is partially successful.
  # eg. Run the apply commands with multiple environments.
  >&2 echo -n $TEST_STDERR
  exit 1
fi

TEST_STDERR="${TEST_STDERR:-Missing stderr either stdout input for the test}"
>&2 echo -n $TEST_STDERR
exit 1
