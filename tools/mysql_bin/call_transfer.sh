#!/bin/bash
FULLPATH="$(dirname $(realpath $0))"
sudo $FULLPATH/transfer.py $1 $2
