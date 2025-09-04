"""
APEL - DELETE stale records from the DB

This script connects to a MySQL database and deletes stale records from a specified table.
Records are only deleted if the number of recent entries (within a user-defined timeframe)
meets or exceeds a threshold.

The script supports `--dry-run` mode.

NOTE:
    Before running this script, ensure the following:

    - A valid configuration file (e.g. delete_stale_records.cfg) is present and accessible.
    - A log file path (e.g. delete_stale_records.log) is defined either in the config
      or via the --log_config argument.
    - The directory for the log file exists and is writable.

    You can override the default paths using:
        --script_config /path/to/delete_stale_records.cfg
        --log_config /path/to/delete_stale_records.log

Usage:
    python delete_stale_records.py
    python delete_stale_records.py --dry-run
    python delete_stale_records.py --script_config /path/to/delete_stale_records.cfg --log_config /path/to/delete_stale_records.log
"""

# Requirements:
# mysqlclient==2.1.1  # This version works with both Python 3.6 and 3.9.

# Installation examples:
# - For Python 3.6 and Python 3.9: python3 -m pip install mysqlclient==2.1.1

import logging
import os
import sys
from argparse import ArgumentParser
from configparser import ConfigParser, NoOptionError, NoSectionError
from datetime import timedelta

import MySQLdb

# Constants
LOG_BREAK = '====================='
# Default config and log file paths
DEFAULT_SCRIPT_CONFIG_PATH = '/etc/apel/delete_stale_records.cfg'
DEFAULT_LOGFILE_PATH = '/var/log/apel/delete_stale_records.log'


def delete_stale_records(config_parser, args):
    """
    Deletes stale records from the configured MySQL table based on UpdateTime.

    Records older than (MAX(UpdateTime) - timeframe) are eligible for deletion,
    but only if the number of recent records (within timeframe) meets the threshold.

    Parameters:
        config_parser: Parsed configuration object.
        args: Parsed command-line arguments.
    """
    conn = None
    cursor = None

    try:
        # Extract database details from the "*.cfg" file
        db_config = {
            'host': config_parser.get('db', 'hostname'),
            'port': config_parser.getint('db', 'port'),
            'user': config_parser.get('db', 'username'),
            'password': config_parser.get('db', 'password'),
            'schema_name': config_parser.get('db', 'schema_name'),
            'table_name': config_parser.get('db', 'table_name'),
            'timeframe': config_parser.getint('common', 'timeframe'),
            'threshold': config_parser.getint('common', 'threshold'),
        }

        # Ensure no required string is empty
        for key in ['host', 'port', 'user', 'password', 'schema_name', 'table_name']:
            if not str(db_config[key]).strip():
                raise ValueError(f"'{key}' in the script config is empty")

        # Numeric validations
        if db_config['timeframe'] <= 0:
            raise ValueError("'timeframe' must be > 0")
        if db_config['threshold'] <= 0:
            raise ValueError("'threshold' must be > 0")

    except (NoSectionError, NoOptionError) as e:
        print(f"Please ensure that the file exists and is readable at: {args.script_config}")
        print(f"Configuration error: {e}")
        sys.exit(1)
    except ValueError as e:
        print(f"Invalid configuration value: {e}")
        sys.exit(1)

    try:
        # Connect to MySQL
        conn = MySQLdb.connect(
            host=db_config['host'],
            port=db_config['port'],
            user=db_config['user'],
            passwd=db_config['password'],
            db=db_config['schema_name']
        )
        cursor = conn.cursor()

        # Verify UpdateTime column exists
        cursor.execute(
            f"SHOW COLUMNS FROM {db_config['table_name']} LIKE 'UpdateTime'"
        )

        if cursor.fetchone() is None:
            log.error(
                f"'UpdateTime' column not found in table "
                f"'{db_config['table_name']}'. Aborting operation."
            )
            return

        # Get the latest UpdateTime value
        cursor.execute(
            f"SELECT MAX(UpdateTime) FROM {db_config['table_name']}"
        )
        result = cursor.fetchone()

        if not result or not result[0]:
            log.warning("No UpdateTime values found. Nothing to purge.")
            return

        max_update = result[0]
        cutoff_time = max_update - timedelta(hours=db_config['timeframe'])
        cutoff_str = cutoff_time.strftime('%Y-%m-%d %H:%M:%S')

        # Count preserved records
        cursor.execute(
            f"SELECT COUNT(*) FROM {db_config['table_name']} WHERE UpdateTime >= %s",
            (cutoff_str,)
        )
        preservable_records_count = cursor.fetchone()[0]

        # Count deletable records
        cursor.execute(
            f"SELECT COUNT(*) FROM {db_config['table_name']} WHERE UpdateTime < %s",
            (cutoff_str,)
        )
        deletable_records_count = cursor.fetchone()[0]

        # Summary log
        summary = (
            f"[SUMMARY] Table: {db_config['table_name']} | "
            f"Timeframe: {db_config['timeframe']}h | "
            f"Threshold: {db_config['threshold']} | "
            f"Preservable Records: {preservable_records_count} | "
            f"Deletable Records: {deletable_records_count} | "
            f"Cutoff: {cutoff_str}"
        )
        log.info(summary)

        # Threshold check
        if preservable_records_count < db_config['threshold']:
            log.warning(
                f"Preservable records ({preservable_records_count}) below threshold "
                f"({db_config['threshold']}). Skipping deletion."
            )
            return


        # Perform deletion or simulate
        if args.dry_run:
            log.info(
                f"DRY_RUN: {deletable_records_count} rows would be deleted from the "
                f"'{db_config['table_name']}' (UpdateTime < {cutoff_str})."
            )
        else:
            log.info("Starting transaction for stale record deletion.")

            cursor.execute(
                f"DELETE FROM {db_config['table_name']} WHERE UpdateTime < %s",
                (cutoff_str,)
            )
            deleted_rows = cursor.rowcount
            conn.commit()
            log.info(
                f"{deleted_rows} rows deleted successfully from the "
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
    # Parse CLI arguments
    parser = ArgumentParser(description='Delete stale records from APEL DB.')
    parser.add_argument(
        '-c', '--script_config',
        help='Location of script config file',
        default=DEFAULT_SCRIPT_CONFIG_PATH
    )
    parser.add_argument(
        '-l', '--log_config',
        help='Location of logging config file',
        default=None
    )
    parser.add_argument(
        '--dry-run',
        dest='dry_run',
        action='store_true',
        help='Preview deletions without executing'
    )
    args = parser.parse_args()

    # Load configuration file
    config_parser = ConfigParser()
    file_read = config_parser.read(args.script_config)
    if not file_read:
        print(f"ERROR: Failed to read script config file: {args.script_config}")
        sys.exit(1)

    # Ensure log directory exists
    LOGFILE_PATH = args.log_config or config_parser.get('logging', 'logfile', fallback=DEFAULT_LOGFILE_PATH)
    LOG_DIR = os.path.dirname(LOGFILE_PATH)
    if not os.path.isdir(LOG_DIR):
        print(f"ERROR: Log directory does not exist: {LOG_DIR}")
        sys.exit(1)

    # Configure logger
    log = logging.getLogger(__name__)
    log.setLevel(logging.INFO)

    if args.dry_run:
        handler = logging.StreamHandler(sys.stdout)
    else:
        handler = logging.FileHandler(LOGFILE_PATH)

    formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')
    handler.setFormatter(formatter)
    log.addHandler(handler)

    log.info(f"{LOG_BREAK}\nStarting APEL DB Purge Script\n")

    delete_stale_records(config_parser, args)
