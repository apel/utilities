"""This file contains a script to fetch and save StAR records."""

import logging
import os
import requests

from dirq.queue import Queue

# Schema for the dirq message queue.
QSCHEMA = {"body": "string", "signer": "string", "empaid": "string?"}
LOG_BREAK = '========================================'
LOG_FILE = "APEL_EUDAT_LOG_FILE"
LOG_LEVEL = logging.INFO


def pull_messages():
    """Pull StAR records from an endpoint and save to a dirq message queue."""
    username = "APEL_EUDAT_USERNAME"
    password = "APEL_EUDAT_PASSWORD"
    hostname = "APEL_EUDAT_HOSTNAME"
    endpoint = "APEL_EUDAT_ENDPOINT"
    qpath = "APEL_EUDAT_QUEUE_PATH"
    inq_path = os.path.join(qpath, 'incoming')
    inq = Queue(inq_path, schema=QSCHEMA)

    try:
        message = requests.get("https://%s/%s" % (hostname, endpoint),
                               auth=(username, password))
    except requests.exceptions.ConnectionError as error:
        # This might happen if the host is incorrect
        logging.error(error)
        return

    body = message.text
    # A bit of a fudge, we cant rely on the endpoint to return an error code
    # if StAR records are not returned, so we have to check the body ourselves.
    if "<sr:StorageUsageRecord>" not in body and "</sr:StorageUsageRecord>" not in body:
        logging.error("Retrieved message does not contain a StAR Record.")
        logging.debug(body)
        return

    # Add the retrieved StAR records to a dirq message queue.
    name = inq.add({"body": body,
                    "signer": hostname,
                    "empaid": "N/A"})

    logging.info("Message saved to incoming queue as %s", name)


if __name__ == "__main__":
    # Set up basic logging
    logging.basicConfig(filename=LOG_FILE, level=LOG_LEVEL,
                        format='%(levelname)s:%(message)s')

    logging.info(LOG_BREAK)
    pull_messages()
    logging.info(LOG_BREAK)
