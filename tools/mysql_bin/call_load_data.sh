#!/bin/bash
FULLPATH="$(dirname $(realpath $0))"
sudo mysql -u root < "$FULLPATH/load_data.sql"
