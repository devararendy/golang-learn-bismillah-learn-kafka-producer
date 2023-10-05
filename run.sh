#!/bin/bash

set -o allexport
source producer.env
set +o allexport

go run . $@