"""
APEL - DELETE stale records from the DB

This script connects to a MySQL database and deletes stale records from a specified table.
Records are only deleted if the number of recent entries (within a user-defined timeframe)
meets or exceeds a threshold.

The script supports dry_run mode.

NOTE:
    Before running this script, ensure the following:

    - A valid configuration file (e.g. delete_stale_records.cfg) is present and accessible.
    - A log file path (e.g. delete_stale_records.log) is defined either in the config
      or via the --log_config argument.
    - The directory for the log file exists and is writable.

    You can override the default paths using:
        --db /path/to/delete_stale_records.cfg
        --log_config /path/to/delete_stale_records.log

Usage:
    python delete_stale_records.py
    python delete_stale_records.py --dry_run
    python delete_stale_records.py --db /path/to/delete_stale_records.cfg --log_config /path/to/delete_stale_records.log
"""

# Requirements:
# mysqlclient==2.1.1  # Latest package; works with Python 3.9+. Dropped support for Python 3.6.
# Installation examples:
# - For Python 3.6: python3.6 -m pip install mysqlclient==2.1.1
# - For Python 3.9: python3.9 -m pip install mysqlclient==2.1.1

import os
import sys
import logging
from datetime import timedelta
from argparse import ArgumentParser
from configparser import ConfigParser, NoSectionError, NoOptionError
import MySQLdb

LOG_BREAK = '====================='


def delete_stale_records(cp, args):
    """
    Deletes stale records from the configured MySQL table based on UpdateTime.

    Records older than (MAX(UpdateTime) - timeframe) are eligible for deletion,
    but only if the number of recent records (within timeframe) meets the threshold.

    Parameters:
        cp: Parsed configuration object.
        args: Parsed command-line arguments.
    """
    conn = None
    cursor = None

    try:
        # Extract database details from the "*.cfg" file
        db_config = {
            'backend': cp.get('db', 'backend'),
            'host': cp.get('db', 'hostname'),
            'port': cp.getint('db', 'port'),
            'user': cp.get('db', 'username'),
            'password': cp.get('db', 'password'),
            'database': cp.get('db', 'name'),
            'table_name': cp.get('db', 'table_name'),
            'timeframe': cp.getint('common', 'timeframe'),
            'threshold': cp.getint('common', 'threshold'),
        }

        # Ensure no required string is empty
        for key in ['backend', 'host', 'user', 'password', 'database', 'table_name']:
            if not db_config[key].strip():
                raise ValueError(f"'{key}' in config is empty")

        # Numeric validations
        if db_config['timeframe'] <= 0:
            raise ValueError("'timeframe' must be > 0")
        if db_config['threshold'] <= 0:
            raise ValueError("'threshold' must be > 0")

    except (NoSectionError, NoOptionError) as e:
        print(f"Configuration error: {e}")
        sys.exit(1)
    except ValueError as e:
        print(f"Invalid configuration value: {e}")
        sys.exit(1)

    try:
        conn = MySQLdb.connect(
            host=db_config['host'],
            port=db_config['port'],
            user=db_config['user'],
            passwd=db_config['password'],
            db=db_config['database']
        )
        cursor = conn.cursor()

        # Verify UpdateTime column exists
        cursor.execute(
            f"SHOW COLUMNS FROM {db_config['table_name']} LIKE 'UpdateTime'"
        )

        if cursor.fetchone() is None:
            column_not_found_error = (
                f"'UpdateTime' column not found in table "
                f"'{db_config['table_name']}'. Aborting operation."
            )

            if args.dry_run:
                print(column_not_found_error)
            else:
                log.error(column_not_found_error)

            return

        # Get the latest UpdateTime value
        cursor.execute(
            f"SELECT MAX(UpdateTime) FROM {db_config['table_name']}"
        )
        result = cursor.fetchone()

        if not result or not result[0]:
            no_records_found_error = "No UpdateTime values found. Nothing to purge."

            if args.dry_run:
                print(no_records_found_error)
            else:
                log.error(no_records_found_error)
            
            return

        max_update = result[0]
        cutoff_time = max_update - timedelta(hours=db_config['timeframe'])
        cutoff_str = cutoff_time.strftime('%Y-%m-%d %H:%M:%S')

        cursor.execute(
            f"SELECT COUNT(*) FROM {db_config['table_name']} WHERE UpdateTime >= %s",
            (cutoff_str,)
        )
        preserved_count = cursor.fetchone()[0]
        deletable_count = 0

        summary = (
            f"[SUMMARY] Table: {db_config['table_name']} | "
            f"Timeframe: {db_config['timeframe']}h | "
            f"Threshold: {db_config['threshold']} | "
            f"Preserved: {preserved_count} | "
            f"Deletable: {deletable_count} | "
            f"Cutoff: {cutoff_str}"
        )

        if preserved_count < db_config['threshold']:
            records_below_threshold_error = (
                f"Preserved records ({preserved_count}) below threshold "
                f"({db_config['threshold']}). Skipping deletion."
            )

            if args.dry_run:
                print(f"{summary} | Action: DRY_RUN")
                print(records_below_threshold_error)
            else:
                log.info(f"{summary} | Action: ABORT")
                log.warning(records_below_threshold_error)
        else:
            cursor.execute(
                f"SELECT COUNT(*) FROM {db_config['table_name']} WHERE UpdateTime < %s",
                (cutoff_str,)
            )
            deletable_count = cursor.fetchone()[0]

            records_deleted_msg = (
                f"{deletable_count} rows deleted successfully from the"
                f"'{db_config['table_name']}' (UpdateTime < {cutoff_str})."
            )

            if not args.dry_run:
                cursor.execute(
                    f"DELETE FROM {db_config['table_name']} WHERE UpdateTime < %s",
                    (cutoff_str,)
                )
                deleted_rows = cursor.rowcount
                conn.commit()

                log.info(f"{summary} | Action: SUCCESS")
                log.info(
                    f"{deletable_count} rows deleted successfully from the"
                    f"'{db_config['table_name']}' (UpdateTime < {cutoff_str})."
                )
            else:
                print(f"{summary} | Action: DRY_RUN")
                print(
                    f"DRY_RUN: {deletable_count} rows would be deleted from the"
                    f"'{db_config['table_name']}' (UpdateTime < {cutoff_str})."
                )

    except MySQLdb.Error as err:
        log.error(f"MySQL error: {err}")
        if conn:
            conn.rollback()
            log.warning("Transaction rolled back due to error.")
    finally:
        if cursor:
            cursor.close()
        if conn:
            conn.close()


if __name__ == '__main__':
    # Default config and log file paths
    default_config_path = '/etc/apel/delete_stale_records.cfg'
    default_logfile_path = '/var/log/apel/delete_stale_records.log'

    # Parse CLI arguments
    parser = ArgumentParser(description='Delete stale records from APEL DB.')
    parser.add_argument(
        '-d', '--db',
        help='Location of DB config file',
        default=default_config_path
    )
    parser.add_argument(
        '-l', '--log_config',
        help='Location of logging config file',
        default=None
    )
    parser.add_argument(
        '--dry_run',
        action='store_true',
        help='Preview deletions without executing'
    )
    args = parser.parse_args()

    # Load configuration file
    cp = ConfigParser()
    read_files = cp.read(args.db)
    if not read_files:
        print(f"Error: Failed to read config file: {args.db}")
        sys.exit(1)

    # Ensure log directory exists
    logfile_path = args.log_config or cp.get('logging', 'logfile', fallback=default_logfile_path)
    log_dir = os.path.dirname(logfile_path)
    if not os.path.isdir(log_dir):
        print(f"Error: Log directory does not exist: {log_dir}")
        sys.exit(1)

    logging.basicConfig(
        filename=logfile_path,
        level=logging.INFO,
        format='%(asctime)s - %(levelname)s - %(message)s'
    )
    log = logging.getLogger(__name__)

    if args.dry_run:
        print(f"{LOG_BREAK}\nStarting APEL DB Purge Script\n")
    else:
        log.info(f"{LOG_BREAK}\nStarting APEL DB Purge Script\n")

    delete_stale_records(cp, args)
