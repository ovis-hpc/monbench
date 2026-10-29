#!/bin/bash

set -e
rpcbind
exec "$@"
