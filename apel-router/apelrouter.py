#!/usr/bin/env python3
"""
APEL Message Router.

This script watches an incoming directory for records. It identifies
the type of each message (XML, JSON, or Text) and moves it to the appropriate
destinations.

Usage:
    python3 apelrouter.py
    python3 apelrouter.py --script_config /path/to/apelrouter.yaml
    python3 apelrouter.py --script_config /path/to/apelrouter.yaml --log_file /path/to/apelrouter.log
"""

# Requirements:
# PyYAML==6.0.1  # This version works with both Python 3.6 and 3.9.
# apel-lib       # Make sure you have `apel-lib` installed in your AQ-managed VM.
#                # i.e. yum list installed | grep apel-lib
#                # If apel-lib is NOT installed then you can install it from https://github.com/apel/apel/releases

# Installation examples:
# - For Python 3.6 and Python 3.9: `python3 -m pip install PyYAML==6.0.1`

import json
import logging
import os
import shutil
import sys
import time
from argparse import ArgumentParser

import yaml
from daemon.daemon import DaemonContext

# Constant variables defined in the APEL.
from apel.db import (
    JOB_MSG_HEADER,
    JOB_MSG_HEADER_04,
    SUMMARY_MSG_HEADER,
    SUMMARY_MSG_HEADER_04,
    NORMALISED_SUMMARY_MSG_HEADER,
    NORMALISED_SUMMARY_MSG_HEADER_04,
    SYNC_MSG_HEADER,
    CLOUD_MSG_HEADER,
    CLOUD_SUMMARY_MSG_HEADER,
)
from apel.db.loader.star_parser import StarParser
from apel.db.loader.xml_parser import get_primary_ns


# --- Custom/New Message Types ---
# Defining strings for message headers that might not be in the APEL yet.
ACCELERATOR_MSG_TYPE = "APEL-accelerator-message: 0.1"
ACCELERATOR_SUMMARY_MSG_TYPE = "APEL-accelerator-summary-message: 0.1"
IP_RECORDS = "APEL Public IP message: 0.2"

# --- CONSTANTS ---
DEFAULT_CONFIG_PATH = "/etc/apel/apelrouter.yaml"
DEFAULT_LOGFILE_PATH = "/var/log/apel/apelrouter.log"
DEFAULT_PIDFILE_PATH = "/var/run/apel/apelrouter.pid"
DEFAULT_INTERVAL = 60  # The number of seconds to sleep between loops.

LOGGER_ID = "router"
LOG_BREAK = "====================="

# Maps the XML namespace URL to a readable key name.
STORAGE_XML_NAMESPACE = StarParser.NAMESPACE
XML_NAMESPACE_TO_KEY = {
    STORAGE_XML_NAMESPACE: "STORAGE_XML_HEADER",
}


def normalize_version(v):
    """Returns the version string exactly as is."""
    if v is None:
        return None
    return str(v).strip()


def read_file_content(path):
    """Reads a text file and returns the stripped content, or None on failure."""
    try:
        # Open file in Read Mode with UTF-8 encoding.
        with open(path, "r", encoding="utf-8") as f:
            # Read content, strip whitespace. If empty string, return None.
            return f.read().strip() or None
    except (OSError, UnicodeError):
        return None


def load_yaml_config(path):
    """Parses the YAML configuration file."""
    # Open the YAML file in read mode.
    with open(path, "r", encoding="utf-8") as f:
        # Parse YAML text into a Python Dictionary.
        # Added 'or {}' to prevent crashes if the file is empty.
        return yaml.safe_load(f) or {}


def resolve_symbolic_name(key):
    """Returns the value of the variable if it exists in globals, else the key itself."""
    # Try to find variable with name 'key'.
    val = globals().get(key)
    return val if isinstance(val, str) else key


def build_header_map(config, log):
    """
    Builds the header mapper dictionary which maps header's to destination path like { 'Header' : '/destination/path', ... }.
    """
    # Initialize empty dictionary for the map.
    headers_to_paths = {}

    # Get the `message_types_with_version` into dictionary from config.
    versioned_config = config.get("message_types_with_version", {})
    # Get the "message_types_without_version" into dictionary from config.
    unversioned_config = config.get("message_types_without_version", {})

    versioned_keys = set(versioned_config.keys())
    unversioned_keys = set(unversioned_config.keys())

    # Check if the user accidentally put the same key in BOTH lists.
    conflict_keys = versioned_keys.intersection(unversioned_keys)

    if conflict_keys:
        for key in conflict_keys:
            # Warn user if they kept the same key in both sections.
            log.warning(
                f"Configuration Ambiguity: Key '{key}' is in both sections. Using versioned configuration."
            )

    # Process Version Match
    for yaml_key, path in versioned_config.items():
        if not yaml_key or not path:
            continue
        resolved_header = resolve_symbolic_name(yaml_key)
        headers_to_paths[resolved_header] = path

    # Process Base Match (Text before colon)
    for yaml_key, path in unversioned_config.items():
        if not yaml_key or not path:
            continue
        resolved_header = resolve_symbolic_name(yaml_key)

        # Split string at colon to remove version.
        base_header = resolved_header.split(":", 1)[0].strip()

        # Only add if not already present. This ensures key isn't overwritten.
        if base_header not in headers_to_paths:
            headers_to_paths[base_header] = path

    # Return the completed map.
    return headers_to_paths


def get_header_from_content(message_body_content, headers_to_paths):
    """
    Inspects message body type and returns the matching Header String from the map.
    """
    content = message_body_content.strip()
    if not content:
        # Error if the file is empty.
        raise ValueError("Unconfigured message type: <empty body>")

    # --- XML ---
    if content.startswith("<"):
        # Helper extracts the Namespace URL from the XML tag.
        namespace = get_primary_ns(content)

        # Check if raw namespace is in our map.
        if namespace in headers_to_paths:
            return namespace

        # Check if namespace maps to a known key using our manual dictionary.
        symbolic_key = XML_NAMESPACE_TO_KEY.get(namespace)
        if symbolic_key and symbolic_key in headers_to_paths:
            return namespace

        raise ValueError("Unconfigured XML message type: %s" % namespace)

    # --- JSON ---
    if content.startswith("{"):
        try:
            data = json.loads(content)
        except json.JSONDecodeError as exc:
            raise ValueError("Unconfigured JSON message type: %s" % exc)

        # Extract 'Type' field.
        msg_type = data.get("Type")
        # Extract 'Version' field.
        msg_version = normalize_version(data.get("Version"))

        if not msg_type:
            raise ValueError("Unconfigured JSON message type: <missing Type>")

        formatted_header = f"{msg_type}: {msg_version}" if msg_version else msg_type

        # Try Exact Match
        if formatted_header in headers_to_paths:
            return formatted_header

        # Try Base Match (Text before colon)
        if msg_type in headers_to_paths:
            return msg_type

        raise ValueError("Unconfigured JSON message type: %s" % formatted_header)

    # --- APEL Format ---
    first_line = content.splitlines()[0].strip()

    # Try Exact Match
    if first_line in headers_to_paths:
        return first_line

    # Try Base Match (Text before colon)
    base_header = first_line.split(":", 1)[0].strip()
    if base_header in headers_to_paths:
        return base_header

    raise ValueError("Unconfigured APEL format message type: %s" % first_line)


def move_to_reject_queue(msg_dir, reject_root, partition, msg_id, log):
    """
    Moves a message to the reject queue. Deletes existing destination if present.
    """
    # Calculate full destination path.
    dest_reject_dir = os.path.join(reject_root, partition, msg_id)
    # Get ID for logging purpose.
    empaid = read_file_content(os.path.join(msg_dir, "empaid")) or "N/A"

    # If the folder already exists, clear it out first.
    if os.path.exists(dest_reject_dir):
        try:
            shutil.rmtree(dest_reject_dir)
            log.error("Destination reject path '%s' exists; replacing it.", dest_reject_dir)
        except (OSError, shutil.Error) as exc:
            log.error("Filesystem: Failed to clear reject path '%s': %s", dest_reject_dir, exc)
            # Try to remove source to prevent processing loop.
            try:
                shutil.rmtree(msg_dir)
                log.info("Removing source message %s/%s (ID=%s) to prevent loop.", partition, msg_id, empaid)
            except OSError as rm_exc:
                log.error("Filesystem: Failed to remove source message %s/%s: %s", partition, msg_id, rm_exc)
            return

    try:
        # Create parent directories.
        os.makedirs(os.path.dirname(dest_reject_dir), exist_ok=True)
        # Move the directory.
        shutil.move(msg_dir, dest_reject_dir)
        # Log success details.
        log.info("Message moved to reject queue as '%s'", dest_reject_dir)
        log.info("Removing message %s/%s. ID = %s", partition, msg_id, empaid)
    except (OSError, shutil.Error) as exc:
        log.error("Filesystem: Error moving message to reject queue '%s': %s", dest_reject_dir, exc)
        try:
            shutil.rmtree(msg_dir)
            log.info("Removing source message %s/%s (ID=%s) after reject failure.", partition, msg_id, empaid)
        except OSError as rm_exc:
            log.error("Filesystem: Failed to remove source message %s/%s: %s", partition, msg_id, rm_exc)


def atomic_move_message(source_dir, destination_queue_path, partition, msg_id):
    """
    Moves a message safely. Tries atomic rename first, falls back to staging.
    Returns the final path if successful, or raises FileExistsError/OSError.
    """
    # Define destination paths.
    destination_partition = os.path.join(destination_queue_path, partition)
    final_destination_path = os.path.join(destination_partition, msg_id)

    # Create destination partition folder.
    os.makedirs(destination_partition, exist_ok=True)

    # Prevent overwriting valid data.
    # Collision Check
    if os.path.exists(final_destination_path):
        raise FileExistsError(final_destination_path)

    # Try Atomic Rename
    try:
        # This works if Source and Dest are on the SAME filesystem.
        os.rename(source_dir, final_destination_path)
        return final_destination_path
    except OSError:
        # Fallback: Staging Strategy
        base_spool_dir = os.path.dirname(destination_queue_path.rstrip(os.sep))
        staging_dir = os.path.join(base_spool_dir, ".router_staging", partition, msg_id)

        # Create staging directory.
        os.makedirs(os.path.dirname(staging_dir), exist_ok=True)
        # Clean up stale partial copy, if present.
        if os.path.exists(staging_dir):
            shutil.rmtree(staging_dir)

        # Copy data to staging.
        shutil.move(source_dir, staging_dir)
        # Atomic rename from staging to final.
        os.rename(staging_dir, final_destination_path)
        return final_destination_path


def process_message_queue(config, headers_to_paths, log):
    """Main routing iteration."""
    # Get configuration paths.
    incoming_path = config.get("common", {}).get("incoming_message_path")
    reject_path = config.get("common", {}).get("rejected_message_path")

    # Ensure paths exist on disk.
    os.makedirs(incoming_path, exist_ok=True)
    os.makedirs(reject_path, exist_ok=True)

    # Scan for messages
    total_messages = 0
    messages_to_process = []

    with os.scandir(incoming_path) as partitions:
        # os.scandir streams entries one by one instead of loading a huge list.
        # Sorting partitions to maintain deterministic order
        for partition_entry in sorted(partitions, key=lambda e: e.name):
            if not partition_entry.is_dir():
                continue

            # Iterate over message directories inside partition.
            with os.scandir(partition_entry.path) as messages:
                for msg_entry in sorted(messages, key=lambda e: e.name):
                    if msg_entry.is_dir():
                        total_messages += 1
                        # Add valid message path to list.
                        messages_to_process.append((partition_entry.name, msg_entry.name, msg_entry.path))

    log.info("Found %d messages", total_messages)
    if not messages_to_process:
        # Return False because there was nothing to do.
        return False

    # Process each message
    for partition, msg_id, msg_dir in messages_to_process:
        # Check if directory still exists (Race condition check).
        if not os.path.exists(msg_dir):
            continue

        # Read ID for logging.
        empaid = read_file_content(os.path.join(msg_dir, "empaid")) or "N/A"
        log.info("Routing message %s/%s. ID = %s", partition, msg_id, empaid)

        # Log signer if present.
        signer = read_file_content(os.path.join(msg_dir, "signer")) or "N/A"
        if signer:
            log.info("Routing message from %s", signer)

        try:
            # Read Body file.
            body_content = read_file_content(os.path.join(msg_dir, "body"))
            if body_content is None:
                raise ValueError("Body file missing or unreadable")

            header = get_header_from_content(body_content, headers_to_paths)

            # Lookup Destination Path.
            destination_queue = headers_to_paths.get(header)

            # Special case for XML namespace
            if destination_queue is None:
                symbolic_key = XML_NAMESPACE_TO_KEY.get(header)
                if symbolic_key:
                    destination_queue = headers_to_paths.get(symbolic_key)
            
            if not destination_queue:
                raise ValueError("Unconfigured message type: %s" % header)

            # Move File Atomically.
            final_path = atomic_move_message(msg_dir, destination_queue, partition, msg_id)

            # Log success.
            log.info("Routed message to '%s' (type=%s)", final_path, header)
            log.info("Removing message %s/%s. ID = %s", partition, msg_id, empaid)

        # --- Exception Handling ---
        except FileExistsError as e:
            # Case: Destination file already exists.
            dest_msg_dir = str(e)
            log.error("Destination path '%s' already exists; rejecting.", dest_msg_dir)
            move_to_reject_queue(msg_dir, reject_path, partition, msg_id, log)
        except ValueError as e:
            # Case: Data issue (Bad JSON, missing body, unknown type).
            log.warning("Message rejected due to: %s", e)
            move_to_reject_queue(msg_dir, reject_path, partition, msg_id, log)

    # Success! We finished the loop without crashing the main process.
    return True


def run_daemon_loop(config, headers_to_paths, log):
    """Daemon lifecycle management."""
    common = config.get("common", {})
    pidfile = os.path.abspath(common.get("pidfile", DEFAULT_PIDFILE_PATH))
    interval = common.get("interval", DEFAULT_INTERVAL)

    # Validate interval.
    if not isinstance(interval, int) or interval <= 0:
        log.error("Interval must be a positive integer.")
        sys.exit(1)

    # Create PID dir.
    pid_dir = os.path.dirname(pidfile)
    os.makedirs(pid_dir, exist_ok=True)

    # Check for existing instance.
    if os.path.exists(pidfile):
        log.error("Pidfile %s already exists. Is the APEL Message Router already running?", pidfile)
        sys.exit(1)

    # Preserve log file handle.
    files_preserve = [h.stream for h in log.root.handlers if hasattr(h, 'stream')]

    # Enter Daemon Context (Background mode).
    with DaemonContext(files_preserve=files_preserve):
        try:
            # Write current PID.
            with open(pidfile, "w") as f:
                f.write(str(os.getpid()) + "\n")
        except OSError as e:
            log.error("Failed to create pidfile %s: %s", pidfile, e)

        log.info("Daemon started; PID %s", str(os.getpid()))

        try:
            while True:
                # We do not catch generic Exception here inside the loop.
                # If process_message_queue fails unexpectedly, it will bubble up
                # to the outer except block, log the error, and EXIT.
                process_message_queue(config, headers_to_paths, log)

                # Sleep before next run.
                time.sleep(interval)

        except SystemExit as e:
            log.info("Router shutting down due to SystemExit: %s", e)
        except KeyboardInterrupt:
            log.info("Router interrupted by KeyboardInterrupt")
        except Exception as e:
            log.critical("CRITICAL DAEMON ERROR: %s", e)
            log.exception("Traceback for Critical Daemon Error")
        finally:
            # Cleanup PID file when daemon stops.
            try:
                if os.path.exists(pidfile):
                    os.remove(pidfile)
            except OSError as e:
                log.warning("Failed to remove pidfile %s: %s", pidfile, e)


if __name__ == "__main__":
    # Setup CLI Arguments.
    parser = ArgumentParser(description="Route APEL messages by record type.")
    parser.add_argument(
        "-c", "--script_config",
        default=DEFAULT_CONFIG_PATH,
        help="Location of YAML config file"
    )
    parser.add_argument(
        "-l", "--log_file",
        default=None,
        help="Location of logging file"
    )
    args = parser.parse_args()

    # Load Config.
    try:
        config = load_yaml_config(args.script_config)
    except (OSError, yaml.YAMLError) as e:
        print("ERROR: Failed to read YAML config file: %s (%s)" % (args.script_config, e))
        sys.exit(1)

    # Setup Logging Path.
    log_path = args.log_file or config.get("logging", {}).get("logfile", DEFAULT_LOGFILE_PATH)
    log_dir = os.path.dirname(log_path)
    if log_dir and not os.path.isdir(log_dir):
        print("ERROR: Log directory does not exist: %s" % log_dir)
        sys.exit(1)

    # Initialise Logger.
    root_logger = logging.getLogger()
    root_logger.setLevel(logging.INFO)
    try:
        handler = logging.FileHandler(log_path)
        handler.setFormatter(logging.Formatter("%(asctime)s - %(name)s - %(levelname)s - %(message)s"))
        root_logger.addHandler(handler)
    except OSError as e:
        print("ERROR: Could not access log file %s: %s" % (log_path, e))
        sys.exit(1)

    log = logging.getLogger(LOGGER_ID)
    log.info("%s", LOG_BREAK)
    log.info("Starting APEL Message Router")

    # Build Header Map.
    try:
        headers_to_paths = build_header_map(config, log)
    except Exception as e:
        log.error("Error building routing header Map: %s", e)
        sys.exit(1)

    # Start Service.
    run_daemon_loop(config, headers_to_paths, log)
