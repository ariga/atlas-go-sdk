#!/bin/bash

TEST_ATLAS_VERSION="${TEST_ATLAS_VERSION:-v1.2.3}"

if [[ $TEST_ATLAS_COMMUNITY_EDITION = "1" ]]; then
    echo "atlas community version $TEST_ATLAS_VERSION"
else
    echo "atlas version $TEST_ATLAS_VERSION"
fi
