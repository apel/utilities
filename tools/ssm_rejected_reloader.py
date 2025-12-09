"""
This script parses a configured APEL reject dirq for messages with a given
DN rejected by SSM for invalid signer and pushes them to a dirq for loading.
"""

from dirq.queue import Queue

from ssm.ssm2 import Ssm2


# Path to dirq folder of messages rejected from SSM
IN_PATH = r"/var/spool/apel/cloud/reject"
# Path to dirq folder for messages that we want to reload
OUT_PATH = r"/var/spool/apel/cloud/incoming"
# List of DNs of messages that we want to accept and reload
ACCEPTED_CERTS = ["/C=UK/O=eScience/OU=Cambridge/L=UCS/CN=gw-wcdc-01.hpc.cam.ac.uk",]
ACCEPTED_ERROR = "Signer not in valid DNs list: /C=UK/O=eScience/OU=Cambridge/L=UCS/CN=gw-wcdc-01.hpc.cam.ac.uk"


class Reloader:
    def __init__(self):
        # Create queue to pull from (rejected messages)
        self._inq = Queue(IN_PATH, schema=Ssm2.REJECT_SCHEMA)
        # Create queue to push to (loader incoming dir)
        self._outq = Queue(OUT_PATH, schema=Ssm2.QSCHEMA)

        # Configure variables used for _handle_msg
        self._cert = r"/etc/grid-security/hostcert.pem"
        self._key = r"/etc/grid-security/hostkey.pem"
        self._capath = r"/etc/grid-security/certificates"
        self._check_crls = False
        self._valid_dns = ACCEPTED_CERTS
    
    def reload_msgs(self):
        current_msg = self._inq.first()
        good_count = 0
        while current_msg:
            #self._inq.unlock(current_msg)
            if not self._inq.lock(current_msg):
                print("Skipping locked message %s" % current_msg)
                current_msg = next(self._inq, None)
                continue

            data = self._inq.get(current_msg)
            if data["signer"] in ACCEPTED_CERTS and data["error"] == ACCEPTED_ERROR:
                extracted_msg, _signer, _err_msg = Ssm2._handle_msg(self, data["body"])
                self._outq.add(
                    {'body': extracted_msg,
                    'signer': data["signer"],
                    'empaid': data["empaid"]}
                )
                
                good_count += 1
            # N.B. Doesn't remove the message from the reject queue
            self._inq.unlock(current_msg)
            current_msg = self._inq.next()
        
        print("Reloaded %s messages" % good_count )


def main():
    reloader = Reloader()
    reloader.reload_msgs()


if __name__ == "__main__":
    main()
