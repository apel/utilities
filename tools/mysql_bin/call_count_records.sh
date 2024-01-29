#!/bin/bash
# Returns only the number of records
FULLPATH="$(dirname $(realpath $0))"
res="`sudo mysql -u root < $FULLPATH/count_records.sql`"
sed -n '2p;4p;6p' <<< $res
