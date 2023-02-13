#!/usr/bin/env bash
# Copyright (c) Facebook, Inc. and its affiliates. All Rights Reserved.
docker run -v $PWD:/rocks -w /rocks buildpack-deps make
