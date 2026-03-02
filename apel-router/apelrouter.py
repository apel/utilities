#!/usr/bin/env python3
"""
APEL Message Router.

This script watches an incoming dirq queqe for records. It identifies
the type of each message (XML, JSON, or Text) and routes it to the appropriate
destinations.

Usage:
    python3 apelrouter.py
    python3 apelrouter.py --script_config /path/to/apelrouter.yaml
    python3 apelrouter.py --script_config /path/to/apelrouter.yaml --log_file /path/to/apelrouter.log
"""

# Requirements:
# PyYAML==6.0.1  # This version works with both Python 3.6 and 3.9.
# apel-lib       # Make sure you have `apel-lib` installed
#                # i.e. yum list installed | grep apel-lib
#                # If apel-lib is NOT installed then you can get from https://github.com/apel/apel/releases and install it
# dirq           # Directory based queue. We need dirq to offer a simple queue system
# python-daemon  # Daemonisation.
# Installation examples:
# - For Python 3.6 and Python 3.9: `python3 -m pip install PyYAML==6.0.1 dirq python-daemon``

import json
import logging
import os
import sys
import time
from argparse import ArgumentParser

import yaml
from daemon.daemon import DaemonContext
from dirq.queue import Queue
from dirq.Exceptions import QueueError

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
# Version numbers deliberately excluded; routing cares about the *type*
# of record, not the version.
ACCELERATOR_MSG_TYPE = "APEL-accelerator-message"
ACCELERATOR_SUMMARY_MSG_TYPE = "APEL-accelerator-summary-message"
IP_RECORDS = "APEL Public IP message"

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

# --- dirq schemas ---
QSCHEMA = {
    "body": "string",
    "empaid": "string?",
    "signer": "string?",
    "error": "string?",
}
REJECT_SCHEMA = {
    "body": "string",
    "signer": "string?",
    "empaid": "string?",
    "error": "string",
}

# Module-level logger.
log = logging.getLogger(LOGGER_ID)


class RouterException(Exception):
    """Exception for use by the Router class."""
    pass


def normalize_version(v):
    """
    Safely converts the input `v` to a string and strips whitespace.

    Returns None if the input is None.
    """
    if v is None:
        return None
    return str(v).strip()


def load_yaml_config(path):
    """Parse and return the YAML configuration file."""
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


def build_header_map(config):
    """
    Builds the header mapper dictionary which maps header's to destination path like { 'Header' : '/destination/path', ... }.
    """
    # Initialize empty dictionary for the map.
    headers_to_paths = {}

    # Get the `message_types_with_version` into dictionary from config.
    versioned = config.get("message_types_with_version", {})
    # Get the "message_types_without_version" into dictionary from config.
    unversioned = config.get("message_types_without_version", {})

    # Check if the user accidentally put the same key in BOTH lists.
    for key in set(versioned) & set(unversioned):
        # Warn user if they kept the same key in both sections.
        
        log.warning(
            "Configuration Ambiguity: Key '%s' is in both sections. "
            "Using versioned configuration.", key,
        )

    # Process Version Match
    for yaml_key, path in versioned.items():
        if not yaml_key or not path:
            continue
        resolved_header = resolve_symbolic_name(yaml_key)
        headers_to_paths[resolved_header] = path

    # Process Base Match (Text before colon)
    for yaml_key, path in unversioned.items():
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


def get_header_from_content(body, headers_to_paths):
    """
    Inspects message body type and returns the matching Header String from the map.
    """
    content = body.strip()
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
            raise ValueError("Missing or Empty required JSON field: 'Type'")

        formatted_header = f"{msg_type}: {msg_version}" if msg_version else msg_type

        # Try Exact Match
        if formatted_header in headers_to_paths:
            return formatted_header

        # Try Base Match (Text before colon)
        if msg_type in headers_to_paths:
            return msg_type

        raise ValueError("Unconfigured JSON message type: %s" %
                         formatted_header)

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


class Router:
    """
    Read messages from an incoming dirq queue, determine the record type,
    and route them to the appropriate destination dirq queue.
    """

    def __init__(self, config, headers_to_paths):
        """Create queue objects and store configuration."""
        common = config.get("common", {})

        self._reject_path = common.get("rejected_message_path")
        self._interval = common.get("interval", DEFAULT_INTERVAL)
        self._pidfile = os.path.abspath(common.get("pidfile", DEFAULT_PIDFILE_PATH))
        self._headers_to_paths = headers_to_paths

        # Create queue objects once.
        self._inq = Queue(common.get("incoming_message_path"), schema=QSCHEMA)
        self._rejectq = Queue(self._reject_path, schema=REJECT_SCHEMA)

        # One destination queue per unique path.
        self._dest_queues = {}
        for path in set(headers_to_paths.values()):
            self._dest_queues[path] = Queue(path, schema=QSCHEMA)

        # Tracked so shutdown() can unlock a message left locked.
        self.current_msg = None

        log.info("Router created.")

    def startup(self):
        """Create a pidfile."""
        if os.path.exists(self._pidfile):
            log.warning("A pidfile %s already exists.", self._pidfile)
            log.warning("Check that the router is not running, "
                        "then remove the file.")
            raise RouterException(
                "The router cannot start while pidfile exists.")
        try:
            with open(self._pidfile, "w") as f:
                f.write(str(os.getpid()))
                f.write("\n")
        except IOError as e:
            log.warning("Failed to create pidfile %s: %s", self._pidfile, e)

    def shutdown(self):
        """Unlock the current message element and remove the pidfile."""
        if self.current_msg:
            try:
                log.info("Unlocking message %s on shutdown.", self.current_msg)
                self._inq.unlock(self.current_msg)
            except OSError as e:
                log.error("Unable to remove lock: %s", e)

        try:
            if os.path.exists(self._pidfile):
                os.remove(self._pidfile)
            else:
                log.warning("pidfile %s not found.", self._pidfile)
        except IOError as e:
            log.warning("Failed to remove pidfile %s: %s", self._pidfile, e)
            log.warning("The router may not start again until it is removed.")

        log.info("The router has shut down.")

    def route_all_msgs(self):
        """
        Read every message from the incoming queue and route it to the
        correct destination queue.
        """
        log.debug(LOG_BREAK)
        log.debug("Starting router run.")

        num_msgs = self._inq.count()
        log.info("Found %s messages", num_msgs)

        self.current_msg = self._inq.first()
        while self.current_msg:
            if not self._inq.lock(self.current_msg):
                log.warning("Skipping locked message %s", self.current_msg)
                self.current_msg = next(self._inq, None)
                continue

            log.debug("Reading message %s", self.current_msg)
            try:
                data = self._inq.get(self.current_msg)
            except QueueError as e:
                log.info("Routing message %s. ID = N/A", self.current_msg)
                log.info("Routing message from N/A")
                log.warning("Message rejected due to: %s", e)
                name = self._rejectq.add({
                    "body": "",
                    "signer": "",
                    "empaid": "",
                    "error": str(e),
                })
                log.info("Message moved to reject queue as '%s'",
                         os.path.join(self._reject_path, name))
                log.info("Removing message %s. ID = N/A",
                         self.current_msg)
                self._inq.remove(self.current_msg)
                self.current_msg = next(self._inq, None)
                continue

            # Read ID for logging (empaid).
            msg_id = data.get("empaid") or "N/A"

            # Log signer if present.
            signer = data.get("signer") or "N/A"
            body = data.get("body", "")

            log.info("Routing message %s. ID = %s", self.current_msg, msg_id)
            log.info("Routing message from %s", signer)

            try:
                if not body:
                    raise ValueError("Body file missing or unreadable")

                header = get_header_from_content(body, self._headers_to_paths)

                # Lookup Destination Path.
                dest_path = self._headers_to_paths.get(header)

                # Special case for XML namespace
                if dest_path is None:
                    sym = XML_NAMESPACE_TO_KEY.get(header)
                    if sym:
                        dest_path = self._headers_to_paths.get(sym)
                if not dest_path:
                    raise ValueError(
                        "Unconfigured message type: %s" % header)

                ename = self._dest_queues[dest_path].add({
                    "body": body,
                    "signer": signer,
                    "empaid": msg_id,
                })
                log.info("Routed message to '%s' (type=%s)",
                         os.path.join(dest_path, ename), header)

            except (ValueError, OSError) as e:
                errmsg = str(e)
                log.warning("Message rejected due to: %s", errmsg)

                name = self._rejectq.add({
                    "body": body,
                    "signer": signer,
                    "empaid": msg_id,
                    "error": errmsg,
                })
                log.info("Message moved to reject queue as '%s'",
                         os.path.join(self._reject_path, name))

            log.info("Removing message %s. ID = %s",
                     self.current_msg, msg_id)
            self._inq.remove(self.current_msg)
            self.current_msg = next(self._inq, None)

        if num_msgs:  # Only tidy up if messages were found.
            log.info("Tidying message directories")
            try:
                self._inq.purge()
                self._rejectq.purge()
            except OSError as e:
                log.warning(
                    "OSError raised while purging message queues: %s", e)

        log.debug("Router run finished.")
        log.debug(LOG_BREAK)


def run_daemon_loop(router):
    """Daemonise and run the routing loop."""
    # Validate interval.
    if not isinstance(router._interval, int) or router._interval <= 0:
        log.critical("Interval must be a positive integer.")
        sys.exit(1)

    # Check for existing instance before daemonising (user sees on stderr).
    if os.path.exists(router._pidfile):
        log.error(
            "Pidfile %s already exists. "
            "Is the APEL Message Router already running?", router._pidfile)
        sys.exit(1)

    # Preserve log file handle.
    files_preserve = [
        h.stream for h in log.root.handlers if hasattr(h, "stream")]

    # Enter Daemon Context (Background mode).
    with DaemonContext(files_preserve=files_preserve):
        try:
            router.startup()
        except RouterException:
            sys.exit(1)

        log.info("Daemon started; PID %s", os.getpid())

        try:
            while True:
                router.route_all_msgs()
                # Sleep before next run.
                time.sleep(router._interval)
        except SystemExit as e:
            log.info("Router shutting down due to SystemExit: %s", e)
        except KeyboardInterrupt:
            log.info("Router interrupted by KeyboardInterrupt")
        except Exception as e:
            log.critical("CRITICAL DAEMON ERROR: %s", e)
            log.exception("Traceback for Critical Daemon Error")
        finally:
            router.shutdown()


if __name__ == "__main__":
    parser = ArgumentParser(description="Route APEL messages by record type.")
    parser.add_argument(
        "-c",
        "--script_config",
        default=DEFAULT_CONFIG_PATH,
        help="Location of YAML config file",
    )
    parser.add_argument(
        "-l",
        "--log_file",
        default=None,
        help="Location of logging file",
    )
    args = parser.parse_args()

    # Initialise logger early with stderr.
    log_format = logging.Formatter(
        "%(asctime)s - %(name)s - %(levelname)s - %(message)s")
    root_logger = logging.getLogger()
    root_logger.setLevel(logging.INFO)

    stderr_handler = logging.StreamHandler(sys.stderr)
    stderr_handler.setFormatter(log_format)
    root_logger.addHandler(stderr_handler)

    # Load Config.
    try:
        config = load_yaml_config(args.script_config)
    except (OSError, yaml.YAMLError) as e:
        print("ERROR: Failed to read YAML config file: %s (%s)" %
              (args.script_config, e))
        sys.exit(1)

    # Setup Logging Path.
    log_path = args.log_file or config.get("logging", {}).get(
        "logfile", DEFAULT_LOGFILE_PATH
    )
    log_dir = os.path.dirname(log_path)
    if log_dir and not os.path.isdir(log_dir):
        print("ERROR: Log directory does not exist: %s", log_dir)
        sys.exit(1)

    try:
        file_handler = logging.FileHandler(log_path)
        file_handler.setFormatter(log_format)
        root_logger.addHandler(file_handler)
    except OSError as e:
        log.critical("Could not access log file %s: %s", log_path, e)
        sys.exit(1)

    log.info(LOG_BREAK)
    log.info("Starting APEL Message Router")

    # Build Header Map.
    try:
        headers_to_paths = build_header_map(config)
    except Exception as e:
        log.critical("Error building routing header map: %s", e)
        sys.exit(1)

    if not headers_to_paths:
        log.critical("No routing rules configured. Nothing to route.")
        sys.exit(1)

    # Create router and start.
    try:
        router = Router(config, headers_to_paths)
    except Exception as e:
        log.critical("Failed to create Router: %s", e)
        sys.exit(1)

    # Start Service.
    run_daemon_loop(router)
