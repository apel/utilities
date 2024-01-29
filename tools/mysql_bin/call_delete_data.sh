#!/bin/bash

FULLPATH="$(dirname $(realpath $0))"

if [[ -z $1 ]]; then
    echo "Usage: $0 (all|new)"
elif [[ "$1" == "new" ]]; then
    DEL="$FULLPATH/delete_new_data.sql"
elif [[ "$1" == "all" ]]; then
    DEL="$FULLPATH/delete_all_data.sql"
fi

sudo mysql -u root < "$DEL"
