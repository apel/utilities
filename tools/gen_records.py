#!/bin/env python3
"""Module for generating test messages for the new APEL system.

It will currently generate job messages or summary messages, depending on
command-line arguments, but shouldn't be difficult to extend to other types of
message.  Create a subclass of RecordGenerator for each message type.
"""

import getopt
import os
import random
import sys
import datetime
from time import mktime, time
import json

# Arbitrary strings for filling messages.
sample_strings = '''Site Navigation
Home
Latest Information
Email Hoaxes
Internet Scams
Previous Issues
Site FAQs
Hoax-Slayer Social
HS About
Privacy Policy
HS Site Map
True Emails
Virus Hoaxes
Giveaway Hoaxes
Charity Hoaxes
Bogus Warnings
Email Petitions
Chain Letters
Unsubstantiated
Missing Child Hoaxes'''.splitlines()

# Some example DNs
dns = ['/C=UK/O=eScience/OU=CLRC/L=RAL/CN=apel-dev.esc.rl.ac.uk/emailAddress=sct-certificates@stfc.ac.uk',
       '/C=UK/O=eScience/OU=CLRC/L=RAL/CN=apel-consumer2.esc.rl.ac.uk/emailAddress=sct-certificates@stfc.ac.uk',
       '/c=cy/o=cygrid/o=hpcl/cn=mon101.grid.ucy.ac.cy',
       '/c=hu/o=niif ca/ou=grid/ou=niif/cn=host/egi1.grid.niif.hu',
       '/dc=org/dc=balticgrid/ou=mif.vu.lt/cn=host/grid9.mif.vu.lt',
       '/dc=es/dc=irisgrid/o=pic/cn=mon01.pic.es',
       '/dc=ro/dc=romaniangrid/o=ifin-hh/cn=tbit03.nipne.ro']

# Possible acceptable values representing null
null_values = ['NULL', 'Null', 'null', 'NONE', 'None', 'none', '']


class RecordGenerator(object):
    """Don't create a RecordGenerator object - create a subclass
    which defines the appropriate header, mandatory and optional
    fields etc."""

    def __init__(self, recs_per_msg, no_msgs):

        if recs_per_msg is None:
            self._recs_per_msg = 100
        else:
            self._recs_per_msg = recs_per_msg
        if no_msgs is None:
            self._no_msgs = 100
        else:
            self._no_msgs = no_msgs
        self._msg_path = ''

        self._header = ''
        self._mandatory_fields = []
        self._all_fields = []
        self._int_fields = []
        self._float_fields = []
        self._datetime_fields = []

        self._field_ranges = {} # Map field to a range # TODO generalise to dns etc

        self._optional_fields = []

        # Label formats for message output
        self._ordered_message_formats = {
            None: self.get_apel_message_ordered,
            'apel': self.get_apel_message_ordered,
            'csv': self.get_csv_message_ordered,
            'json': self.get_json_message_ordered
        }

        # Label records for message output
        self._record_methods = {
            None: self._get_full_record,
            'full': self._get_full_record,
            'minimal': self._get_minimal_record,
            'valid_none': self._get_valid_none_record,
            'incomplete': self._get_incomplete_record,
            'buffered': self._get_buffered_record
        }

        self._get_record_method = 'valid_none'
        self._buffered_records = [None]


    def _get_optional_fields(self):

        # optional fields = all fields - mandatory fields
        self._optional_fields = [s for s in self._all_fields]
        for item in self._mandatory_fields:
            self._optional_fields.remove(item)

    def _get_record(self, keys, job_id):
        """Get a record with all of the keys listed in keys."""
        record = {}
        for key in keys:
            if key in self._int_fields:
                if key in self._field_ranges:
                    lower = self._field_ranges[key][0]
                    upper = self._field_ranges[key][1]
                else:
                    lower = 0
                    upper = 1000000

                record[key] = get_random_int(lower, upper)

            elif key in self._float_fields:
                record[key] = get_random_float()
            elif key in self._datetime_fields:
                # Random time in last year
                random_timestamp = int(time()) - get_random_int(0, 86400*365) # TODO read field range re dates
                record[key] = timestamp_to_utc(random_timestamp)
            else:
                record[key] = get_random_string(sample_strings)
        record['job_id'] = job_id

        return record

    def _get_full_record(self, job_id):
        """Get a record string with all possible fields."""
        return self._get_record(self._all_fields, job_id)

    def _get_minimal_record(self, job_id):
        """Get a record string with only the necessary fields."""
        return self._get_record(self._mandatory_fields, job_id)

    def _get_incomplete_record(self, job_id):
        """Get a record without one of the mandatory fields."""
        # copy the list
        all_keys = [s for s in self._all_fields]
        # remove a mandatory item
        to_remove = get_random_string(self._mandatory_fields)
        all_keys.remove(to_remove)

        return self._get_record(all_keys, job_id)

    def _get_valid_none_record(self, job_id):
        """Get a record giving one of the optional fields null values."""
        rec_dict = self._get_record(self._all_fields, job_id)

        to_edit = get_random_string(self._optional_fields)
        rec_dict[to_edit] = get_random_string(null_values)

        return rec_dict

    def load_record_buffer(self, records):
        """ Adjust num messages to account for size of buffer. """
        bufferlen = len(self._buffered_records)
        self.__buffer_ind = 0
        self._no_msgs = bufferlen // self._recs_per_msg +1
        self._buffered_records = records

    def _get_buffered_record(self, *args):
        """ Allow records to be set externally """
        if self.__buffer_ind < len(self._buffered_records):
            next_record = self._buffered_records[self.__buffer_ind]
            self.__buffer_ind += 1
            return next_record
        else:
            return None

    def get_apel_message(self, prefix, record_method='full'):
        """Get a valid message string."""
        message = self._header + "\n"
        for i in range(self._recs_per_msg):
            dict = self._record_methods[record_method](prefix + str(i)) # TODO rejecting based on single record failing.
            for key in dict.keys():
                message += key
                message += ": "
                message += dict[key]
                message += "\n"
            message += "%%\n"
        return message

    def get_json_message(self, prefix, record_method='full'):
        """ Get valid json message as string."""

        JSON_MSG_DICT = dict(Type=self._msg_type, Version=self._msg_version, 
                             UsageRecords=[])

        for i in range(self._recs_per_msg):
            record = self._record_methods[record_method](prefix + str(i))

            if not record: # Handle end of buffered message
                return message

            JSON_MSG_DICT['UsageRecords'].append(record)

        return str(JSON_MSG_DICT).replace('"', "'")

    def get_apel_message_ordered(self, prefix, record_method='full'):
        """Get a valid message string, with its fields in the correct order."""
        message = self._header + "\n"
        for i in range(self._recs_per_msg):
            dict = self._record_methods[record_method](prefix + str(i))

            if not dict: # Handle end of buffered message
                return message

            # go through in the order of all_fields
            for key in self._all_fields:

                if key in dict.keys():
                    message += key
                    message += ": "
                    message += str(dict[key])
                    message += "\n"
            message += "%%\n"
        return message

    def get_json_message_ordered(self, prefix, record_method='full'):
        """ Get valid json message as string, with its fields in the correct order."""

        JSON_MSG_DICT = dict(Type=self._msg_type, Version=self._msg_version, 
                             UsageRecords=[])

        for i in range(self._recs_per_msg):
            record = self._record_methods[record_method](prefix + str(i))
            # TODO record = self._get_valid_none_record(prefix + str(i))

            if not record: # Handle end of buffered message
                return message

            record_ordered = {key: record[key] for key in self._all_fields if key in record}
            JSON_MSG_DICT['UsageRecords'].append(record_ordered)

        #return str(JSON_MSG_DICT).replace("'", '"')
        return json.dumps(JSON_MSG_DICT, indent=4).replace("'", '"')

    def get_apel_message_lowercase(self, prefix, record_method='full'):
        """Get a message with its key-value pairs in lower-case."""
        message = self._header + "\n"
        for i in range(self._recs_per_msg):
            dict = self._record_methods[record_method](prefix + str(i))

            if not dict: # Handle end of buffered message
                return message

            for key in dict.keys():
                message += key.lower()
                message += ": "
                message += str(dict[key]).lower()
                message += "\n"
            message += "%%\n"
        return message

    def get_csv_message_ordered(self, prefix, record_method='full'):
        """Get a valid csv string, with its columns in the correct order."""
        message = '# ' + self._header + "\n"

        message += '# Type: csv\n'

        dict = self._record_methods[record_method](prefix + '0')
        message += ','.join([k for k in self._all_fields if k in dict]) + '\n'

        delim = ','

        for i in range(self._recs_per_msg-1): # TODO does not write when cutting records-per-msg short
            dict = self._record_methods[record_method](prefix + str(i))

            if not dict: # Handle end of buffered message
                return message

            message_list = [str(dict[k]) for k in self._all_fields if k in dict]
            
            # Check if delim already exists within message
            if [m for m in message_list if delim in m]:
                save = delim
                delim = '\t' # Choose alternative delimiter
                message.replace(save, delim)
            
            message += delim.join(message_list) + '\n'

        return message

    def get_json_message_lowercase(self, prefix, record_method='full'):
        """Get a json message with its key-value pairs in lower-case."""

        JSON_MSG_DICT = dict(Type=self._msg_type, Version=self._msg_version, 
                             UsageRecords=[])

        for i in range(self._recs_per_msg):
            record = self._record_methods[record_method](prefix + str(i))

            if not record: # Handle end of buffered message
                return message

            record_ordered = {key.lower(): record[key].lower() for key in self._all_fields if key in record}
            JSON_MSG_DICT['UsageRecords'].append(record_ordered)

        return json.dumps(JSON_MSG_DICT, indent=4)

    def write_messages(self, fmt=None, method=None):
        """Write the specified number of messages to the specified directory."""

        print(f"Creating {str(self._no_msgs)} messages with {str(self._recs_per_msg)} records each\
 of type {type(self).__name__}.")

        if fmt not in self._ordered_message_formats:
            print(f'ERROR: Format {fmt} not available.')
            print(f'Valid formats include: {list(self._ordered_message_formats.keys())}')
            exit(1)

        if not os.path.exists(self._msg_path):
            print("Creating directory:", self._msg_path, "...")
            os.makedirs(self._msg_path)

        print("Writing to directory:", self._msg_path, "...")

        for i in range(self._no_msgs):
            prefix = get_prefix(i)
            filepath = os.path.join(self._msg_path, str(i).zfill(14))
            k = self._ordered_message_formats[fmt](prefix, record_method=method)
            f = open(filepath, 'w')
            f.write(k)
            f.close()

        print("Done.")


class JobRecordGenerator(RecordGenerator):
    """Generates job record messages for testing the new APEL system."""

    def __init__(self, recs_per_msg, no_msgs, dir):
        """Call the parent constructor, then set up specific variables."""
        super(JobRecordGenerator, self).__init__(recs_per_msg, no_msgs)
        # Variables which control the operation of the generator.

        if dir is None:
            self._msg_path = "job-msgs"
        else:
            self._msg_path = dir
        self._msg_path = os.path.abspath(self._msg_path)

        self._header = "APEL-individual-job-message: v0.2"

        # Fields which are required by the message format.
        self._mandatory_fields = ["SiteID", "SubmitHostID", "LocalJobId", "WallDuration",
                    "CpuDuration", "StartTime", "EndTime", "ServiceLevelType",
                    "ServiceLevel"]

        # All fields in the standard order
        self._all_fields  = ["SiteID", "SubmitHostID", "MachineNameID", "QueueID",
                        "LocalJobId", "LocalUserId",
                       "GlobalUserNameID", "FQAN", "VOID", "VOGroupID", "VORoleID", 
                       "WallDuration", "CpuDuration", "NodeCount", "Processors", 
                       "MemoryReal", "MemoryVirtual", "StartTime", "EndTime",
                        "EndYear", "EndMonth",
                        "InfrastructureDescription", "InfrastructureType",
                       "ServiceLevelType", "ServiceLevel"]

        # Fields whose values should be integers
        self._int_fields = ["GlobalUserNameID","WallDuration", "CpuDuration", "SubmitHostID", "LocalJobId",
                      "Processors", "NodeCount", "MachineNameID",
                      "QueueID", "MemoryReal", "MemoryVirtual", "SiteID", 'VOID',
                      'VOGroupID', 'VORoleID']

        # Fields whose values should be integers
        self._float_fields = ["ServiceLevel"]

        self._datetime_fields = ['StartTime', 'EndTime']

        # Some example FQANs, some of which aren't actual FQANs.
        self._fqans = ['/atlas/higgs/Role=NULL/Capability=NULL',
                       '/cms/uscms/Role=cmsphedex',
                       '/cms/uscms',
                       '/not a real fqan',
                       'junk']

        self._factors = ['HEPSPEC', 'Si2k']

        self._field_ranges['SiteID'] = [1, 10]
        self._field_ranges['SubmitHostID'] = [1, 10]

        RecordGenerator._get_optional_fields(self)

    def _get_record(self, keys, job_id):
        """Add job-specific items to the record after calling the generic get_record() method."""
        # Call parent class method
        record = RecordGenerator._get_record(self, keys, job_id)
        record['GlobalUserName'] = get_random_string(dns)
        record['FQAN'] = get_random_string(self._fqans)
        record['LocalJobId'] = job_id
        record['ServiceLevelType'] = get_random_string(self._factors) 

        start = utc_to_timestamp(record['StartTime'])
        finish = utc_to_timestamp(record['EndTime'])

        if start > finish:
            finish = start + get_random_int(1, 1000)
            record['EndTime'] = timestamp_to_utc(finish)

        return record


class SummaryRecordGenerator(RecordGenerator):
    """Generator for summary messages, defining parts specific to these."""

    def __init__(self, recs_per_msg, no_msgs, msg_path):
        """Define constants used by the summary records."""

        super(SummaryRecordGenerator, self).__init__(recs_per_msg, no_msgs)

        if msg_path is None:
            self._msg_path = "summary-msgs"
        else:
            self._msg_path = msg_path
        self._msg_path = os.path.abspath(self._msg_path)

        self._header = "APEL-summary-job-message: v0.2"

        # Fields which are required by the message format.
        self._mandatory_fields = ["Site", "Month", "Year", "WallDuration",
                "CpuDuration", "NormalisedWallDuration", "NormalisedCpuDuration",
                "NumberOfJobs"]

        # All fields in the standard order
        self._all_fields = ["Site", "Month", "Year", "GlobalUserName", "Group", "VOGroup",
                       "VORole", "EarliestEndTime", "LatestEndTime", "WallDuration", "CpuDuration", "NormalisedWallDuration",
                       "NormalisedCpuDuration", "NumberOfJobs"]

        # Fields whose values should be integers, except EarliestEndTime and LatestEndTime
        self._int_fields = ["Month", "Year", "WallDuration", "CpuDuration",
                      "NormalisedWallDuration", "NormalisedCpuDuration",
                      "NumberOfJobs"]

        # Fields whose values should be integers
        self._float_fields = []

        RecordGenerator._get_optional_fields(self)

    def _get_record(self, keys, job_id):
        """Get a record, then add summary-specific items."""
        record = RecordGenerator._get_record(self, keys, job_id)
        record['GlobalUserName'] = get_random_string(dns)
        record['Month'] = str(get_random_int(end=12))
        record['Year'] = str(get_random_int(2000, 2010))

        # The rest of this method is to get EarliestEndTime and
        # LatestEndTime to fall within the correct month.
        month_start = datetime.datetime(int(record['Year']),
                                        int(record['Month']), 1)
        month_end = month_start + datetime.timedelta(28)

        start_epoch = mktime(month_start.timetuple())
        end_epoch = mktime(month_end.timetuple())

        rnd_epoch1 = get_random_int(start_epoch, end_epoch)
        rnd_epoch2 = get_random_int(start_epoch, end_epoch)

        if rnd_epoch1 > rnd_epoch2:
            record['EarliestEndTime'] = str(rnd_epoch2)
            record['LatestEndTime'] = str(rnd_epoch1)
        else:
            record['EarliestEndTime'] = str(rnd_epoch1)
            record['LatestEndTime'] = str(rnd_epoch2)
        return record


class SpecRecordGenerator(RecordGenerator):
    """Generator for Spec messages"""
    #SiteID            INT NOT NULL
    #CEID              INT NOT NULL
    #StartTime         DATETIME
    #StopTime          DATETIME
    #ServiceLevelType  VARCHAR(50) NOT NULL
    #ServiceLevel      DECIMAL(10,3)

    def __init__(self, recs_per_msg, no_msgs, msg_path):
        """Define constants used by the summary records."""

        super(SpecRecordGenerator, self).__init__(recs_per_msg, no_msgs)

        if msg_path is None:
            self._msg_path = "spec-msgs"
        else:
            self._msg_path = msg_path
        self._msg_path = os.path.abspath(self._msg_path)

        self._header = "Uninplemented"

        # Fields which are required by the message format.
        self._mandatory_fields = ['SiteID', 'CEID', 'ServiceLevelType']

        # All fields in the standard order
        self._all_fields = ['SiteID', 'CEID', 'StartTime', 'StopTime', 
                            'ServiceLevelType', 'ServiceLevel']

        # Fields whose values should be integers, except EarliestEndTime and LatestEndTime
        self._int_fields = ['SiteID', 'CEID']

        self._datetime_fields = ['StartTime', 'StopTime']

        # Fields whose values should be integers
        self._float_fields = ['ServiceLevel']

        RecordGenerator._get_optional_fields(self)

    def _get_record(self, keys, job_id):
        """Get a record, then add spec-specific items."""
        record = RecordGenerator._get_record(self, keys, job_id)

        start = utc_to_timestamp(record['StartTime'])
        finish = utc_to_timestamp(record['StopTime'])

        if start > finish:
            record['StopTime'], record['StartTime'] = timestamp_to_utc(start), timestamp_to_utc(finish)

        return record


class BlahdRecordGenerator(RecordGenerator):
    """Generator for Blahd messages"""
    #GlobalUserNameID    int             not null
    #FQAN                varchar(255)    null
    #VOID                int             not null
    #VOGroupID           int             not null
    #VORoleID            int             not null
    #CEID                int             not null
    #GlobalJobId         varchar(255)    null
    #LrmsId              varchar(255)    null
    #SiteID              int             not null
    #ValidFrom           datetime        null
    #ValidUntil          datetime        null    
    #Processed           int             null

    def __init__(self, recs_per_msg, no_msgs, msg_path):
        """Define constants used by the summary records."""

        super(BlahdRecordGenerator, self).__init__(recs_per_msg, no_msgs)

        if msg_path is None:
            self._msg_path = "blahd-msgs"
        else:
            self._msg_path = msg_path
        self._msg_path = os.path.abspath(self._msg_path)

        self._header = "Uninplemented"

        # All fields in the standard order
        self._all_fields = ['TimeStamp', 'GlobalUserNameID', 'FQAN', 'VOID', 'VOGroupID', 'VORoleID',
                            'CEID', 'GlobalJobId', 'LrmsId', 'SiteID', 'ValidFrom', 
                            'ValidUntil', 'Processed']

        # Fields which are required by the message format.
        self._mandatory_fields = ['GlobalUserNameID', 'VOID', 'VOGroupID', 'VORoleID',
                                    'CEID', 'SiteID']

        # Fields whose values should be integers, except EarliestEndTime and LatestEndTime
        self._int_fields = ['GlobalUserNameID', 'VOID', 'VOGroupID', 'VORoleID', 'CEID', "LrmsId",
                             'SiteID', 'Processed']

        self._datetime_fields = ['TimeStamp', 'ValidFrom', 'ValidUntil']

        self._field_ranges['Processed'] = [0, 0] # TODO this should normally be 0, 1, but leave 0, 0 for now

        # Fields whose values should be integers
        self._float_fields = []

        RecordGenerator._get_optional_fields(self)

    def _get_record(self, keys, job_id):
        """Get a record, then add blahd-specific items."""
        record = RecordGenerator._get_record(self, keys, job_id)

        start = utc_to_timestamp(record['ValidFrom'])
        finish = utc_to_timestamp(record['ValidUntil'])

        if start > finish:
            record['ValidUntil'], record['ValidFrom'] = timestamp_to_utc(start), timestamp_to_utc(finish)

        return record


class EventRecordGenerator(RecordGenerator):
    """Generator for Event messages"""
    #SiteID          INT             NOT NULL
    #JobName         VARCHAR(60)     NOT NULL
    #LocalUserID     VARCHAR(20)        
    #LocalUserGroup  VARCHAR(20)
    #WallDuration    INT
    #CpuDuration     INT
    #StartTime       DATETIME        NOT NULL
    #EndTime         DATETIME        NOT NULL
    #Infrastructure  VARCHAR(100)
    #MachineNameID   INT             NOT NULL
    #QueueID         INT             NOT NULL
    #MemoryReal      BIGINT
    #MemoryVirtual   BIGINT
    #Processors      INT
    #NodeCount       INT
    #Status          INT

    def __init__(self, recs_per_msg, no_msgs, msg_path):
        """Define constants used by the summary records."""

        super(EventRecordGenerator, self).__init__(recs_per_msg, no_msgs)

        if msg_path is None:
            self._msg_path = "event-msgs"
        else:
            self._msg_path = msg_path
        self._msg_path = os.path.abspath(self._msg_path)

        self._header = "Uninplemented"

        # Fields which are required by the message format.
        self._mandatory_fields = ['SiteID', 'JobName', 'StartTime', 'EndTime',
                                  'MachineNameID', 'QueueID']

        # All fields in the standard order
        self._all_fields = ['SiteID', 'JobName', 'LocalUserID', 'LocalUserGroup', 'WallDuration',
                            'CpuDuration', 'StartTime', 'EndTime', 'Infrastructure', 'MachineNameID',
                            'QueueID', 'MemoryReal', 'MemoryVirtual', 'Processors', 'NodeCount', 
                            'Status']

        # Fields whose values should be integers, except EarliestEndTime and LatestEndTime
        self._int_fields = ['SiteID', 'WallDuration', 'CpuDuration', 'MachineNameID', 'QueueID', 
                            'MemoryReal', 'MemoryVirtual', 'Processors', 'NodeCount', 'Status',
                            'JobName']

        self._datetime_fields = ['StartTime', 'EndTime']

        # Fields whose values should be integers
        self._float_fields = []

        self._field_ranges['Status'] = [0, 2]

        RecordGenerator._get_optional_fields(self)

    def _get_record(self, keys, job_id):
        """Get a record, then add blahd-specific items."""
        record = RecordGenerator._get_record(self, keys, job_id)

        start = utc_to_timestamp(record['StartTime'])
        finish = utc_to_timestamp(record['EndTime'])

        if start > finish:
            record['EndTime'], record['StartTime'] = timestamp_to_utc(start),  timestamp_to_utc(finish)

        return record


class AcceleratorRecordGenerator(RecordGenerator):
    """Generator for Accelerator messages."""

    def __init__(self, recs_per_msg, no_msgs, msg_path):
        """Define constants used by the Accelerator records."""

        super(AcceleratorRecordGenerator, self).__init__(recs_per_msg, no_msgs)

        if msg_path is None:
            self._msg_path = "acc-msgs"
        else:
            self._msg_path = msg_dir
        self._msg_path = os.path.abspath(self._msg_path)

        self._msg_type = "APEL-Accelerator-message"
        self._msg_version = "0.1"

        # Fields which are required by the message format.
        self._mandatory_fields = [
            "SiteName", "GlobalUserName", "FQAN",
            "Count", "Cores", "AvailableDuration", "Type", "Model",
            "AssociatedRecordType", "ActiveDuration", "MeasurementYear",
            "MeasurementMonth"
        ]

        # This list allows us to specify the order of lines when we construct
        # records.
        self._all_fields = [
            "MeasurementMonth", "MeasurementYear", 
            "AssociatedRecordType", "AssociatedRecord", 
            "GlobalUserName", "FQAN", "SiteName", 
            "Count", "Cores", "ActiveDuration", "AvailableDuration", 
            "BenchmarkType", "Benchmark", 
            "Type", "Model",
            #"PublisherDNID"
        ]

        self._int_fields = ["MeasurementMonth", "MeasurementYear",
                            "ActiveDuration", "AvailableDuration",
                            "Cores"]

        self._float_fields = ["Count", "Benchmark"] 

        self._allowed_types = ['GPU', 'FPGA', 'Other']
        self._allowed_record_types = ['cloud',]


        RecordGenerator._get_optional_fields(self)


    def _get_record(self, keys, job_id):
        """Get a record, then add summary-specific items."""
        record = RecordGenerator._get_record(self, keys, job_id)
        record['GlobalUserName'] = get_random_string(dns)
        record['MeasurementMonth'] = get_random_int(end=12)
        record['MeasurementYear'] = get_random_int(2000, 2021)

        #record['Type'] = mix_enums(self._allowed_types)
        #record['associatedRecordType'] = mix_enums(self._allowed_record_types)

        record['Type'] = get_random_string(self._allowed_types)
        record['AssociatedRecordType'] = get_random_string(self._allowed_record_types)

        return record


class AcceleratorSummaryGenerator(RecordGenerator):
    """Generator for Accelerator summaries."""

    def __init__(self, recs_per_msg, no_msgs, msg_path):
        """Define constants used by the Accelerator records."""

        super(AcceleratorSummaryGenerator, self).__init__(recs_per_msg, no_msgs)

        if msg_path is None:
            self._msg_path = "accsummary-msgs"
        else:
            self._msg_path = msg_dir
        self._msg_path = os.path.abspath(self._msg_path)

        self._msg_type = "APEL-Accelerator-summary-message"
        self._msg_version = "0.1"

        # Fields which are required by the message format.
        self._mandatory_fields = [
            'Month', 'Year', 'AssociatedRecordType', 'SiteName', 'Count', 
            'AvailableDuration', 'Type', 'NumberOfRecords',
        ]

        # This list allows us to specify the order of lines when we construct
        # records.
        self._all_fields = [
            'Month', 'Year', 'AssociatedRecordType', 'GlobalUserName', 'SiteName',
            'Count', 'Cores', 'AvailableDuration', 'ActiveDuration', 'BenchmarkType',
            'Benchmark', 'Type', 'Model', 'NumberOfRecords',
        ]

        self._int_fields = [
            'Month', 'Year', 'Cores', 'AvailableDuration', 'ActiveDuration',
            'NumberOfRecords'
        ]

        self._float_fields = [                
            'Count',
            'Benchmark'
        ] 

        self._allowed_types = ['GPU', 'FPGA', 'Other']
        self._allowed_record_types = ['cloud']


        RecordGenerator._get_optional_fields(self)


    def _get_record(self, keys, job_id):
        """Get a record, then add summary-specific items."""
        record = RecordGenerator._get_record(self, keys, job_id)
        record['GlobalUserName'] = get_random_string(dns)
        record['Month'] = get_random_int(end=12)
        record['Year'] = get_random_int(2000, 2021)

        #record['Type'] = mix_enums(self._allowed_types)
        #record['associatedRecordType'] = mix_enums(self._allowed_record_types)

        record['Type'] = get_random_string(self._allowed_types)
        record['AssociatedRecordType'] = get_random_string(self._allowed_record_types)

        return record


class LinkedRecordGenerator():
    """ Coordinates generator classes to match specific entries.

    Note: This class uses the generator classes but is not supposed to inherit from them.
    """

    record_generators = {
        'job':JobRecordGenerator,
        'summary':SummaryRecordGenerator,
        'accelerator':AcceleratorRecordGenerator,
        'acceleratorsummary':AcceleratorSummaryGenerator,
        'spec':SpecRecordGenerator,
        'blahd':BlahdRecordGenerator,
        'event':EventRecordGenerator
    }

    def __init__(self, recs_per_msg, no_msgs, msgs_root,
                 record_types=[], linked_fields=[]):

        #super(LinkedRecordGenerator, self).__init__(recs_per_msg, no_msgs)

        self._linked_record_types = record_types
        self._linked_record_fields = linked_fields

        if msgs_root is None:
            self._msgs_root = './linked-msgs'
        else:
            self._msgs_root = msgs_root

        if recs_per_msg is None:
            self._recs_per_msg = 100
        else:
            self._recs_per_msg = recs_per_msg

        if no_msgs is None:
            self._no_msgs = 100
        else:
            self._no_msgs = no_msgs

        for lr in self._linked_record_types:
            if lr not in LinkedRecordGenerator.record_generators:
                raise ValueError(f'{lr} not present in available record generators:\n{list(records.keys())}')

        self._init_record_generators()
    
    def _gen_type_ind(self, gen_name):
        return self._linked_record_types.index(gen_name)

    def _init_record_generators(self):
        self._linked_record_generators = [LinkedRecordGenerator.record_generators[lr]
                                          (self._recs_per_msg, self._no_msgs, None)
                                          for lr in self._linked_record_types]

        for lri, lrg in enumerate(self._linked_record_generators):
            lrg._msg_path = os.path.abspath(os.path.join(self._msgs_root, os.path.basename(lrg._msg_path)))

    def _get_linked_record(self, job_id, record_method=None):
        """ Starting with record generator 0, make linked fields of successive records match up """

        # Generate a single record for each record type
        # e.g., [<JobRecord>, <EventRecord>, <BlahdRecord>, <SpecRecord>] 
        linked_record = [lrg._record_methods[record_method](job_id) 
                                for lrg in self._linked_record_generators]

        # Loop through linked record fields
        # For each linked record, make all entries match up,
        # starting with the first non-empty one as the 'master'.
        for record_field_tuple in self._linked_record_fields:
            master_field_chosen = False
            for fi, field in enumerate(record_field_tuple):
                if field: # If field entry is occupied, apply link
                    if not master_field_chosen: # Master field is first field
                        master_field_chosen = True
                        master_field = field
                        mfi = fi
                else: 
                    continue

                # Match up records
                lr_value = linked_record[fi][field]
                mlr_value = linked_record[mfi][master_field]

                # If types don't match in some way
                if type(lr_value) == type(mlr_value):
                    linked_record[fi][field] = mlr_value
                else:
                    print(f'ERROR: linked values <{field}={lr_value}> ({type(lr_value)}), and <{master_field}={mlr_value}> ({type(mlr_value)}) are not of the same type for\
                            \n\r{type(self._linked_record_generators[fi]).__name__} and {type(self._linked_record_generators[mfi]).__name__}')
                    exit(1)

        return linked_record

    def _generate_linked_records(self):
        # Generate a list of record lists separated by type, ready to write to messages

        # Generate a list of linked record
        _linked_record_list = [self._get_linked_record(i, record_method='full') 
                               for i in range(self._no_msgs*self._recs_per_msg)]

        # Convert inner lists from linked records to lists matched by record types
        self._linked_records = [[] for i in self._linked_record_types]
        for linked_record in _linked_record_list:
            for ri, record in enumerate(linked_record):
                self._linked_records[ri].append(record)

        return self._linked_records

    def write_messages(self, msg_fmt):

        # Create a list of 
        self._generate_linked_records()

        # Manually override write_messages process
        print(f'Creating {len(self._linked_record_generators)} record types.')
        for which_generator, generator in enumerate(self._linked_record_generators):

            # Create a record "buffer" generator object for each record generator
            # RecordGenerator._get_buffered_record(...) is called to return a record
            i = list(self._linked_records[which_generator])

            generator.load_record_buffer(self._linked_records[which_generator])
            generator.write_messages(msg_fmt, method='buffered')
        

class JoinJobRecordsGenerator(LinkedRecordGenerator):
    """Generate linked records for testing JoinJobRecords"""

    def __init__(self, recs_per_msg, no_msgs, msgs_root):
            
        # Job records are built from Event, Blahd, and Spec records
        record_types = ['job', 'event', 'blahd', 'spec']

        # How are job fields linked to latter type fields?
        linked_fields = [
            ('SiteID', 'SiteID', 'SiteID', 'SiteID'),
            ('SubmitHostID', '','CEID','CEID'),
            ('MachineNameID', 'MachineNameID','',''),
            ('QueueID', 'QueueID','',''),
            ('LocalJobId', 'JobName','LrmsId',''),
            ('LocalUserId', 'LocalUserID','',''),
            ('GlobalUserNameID', '','GlobalUserNameID',''),
            ('FQAN', '','FQAN',''),
            ('VOID', '','VOID',''),
            ('VOGroupID', '','VOGroupID',''),
            ('VORoleID', '','VORoleID',''),
            ('WallDuration', 'WallDuration','',''),
            ('CpuDuration', 'CpuDuration','',''),
            ('Processors', 'Processors','',''),
            ('NodeCount', 'NodeCount','',''),
            ('StartTime', 'StartTime','',''),
            ('EndTime', 'EndTime','',''),
            ('InfrastructureDescription', 'Infrastructure','',''),
            ('MemoryReal', 'MemoryReal','',''),
            ('MemoryVirtual', 'MemoryVirtual','',''),
            ('ServiceLevelType', '','','ServiceLevelType'),
            ('ServiceLevel', '','','ServiceLevel')
        ]

        super(JoinJobRecordsGenerator, self).__init__(recs_per_msg, no_msgs, msgs_root,
                                            record_types=record_types,
                                            linked_fields=linked_fields)

    def _generate_linked_records(self):
        # Account for the one-to-many mapping between SpecRecords and JobRecords
        
        linked_records = super(JoinJobRecordsGenerator, self)._generate_linked_records()

        # For each unique combination of SiteID and SubmitHostID/CEID, have a SpecRecord
        # ...
        siteid_ceid_pairs = []
        job_records = linked_records[self._gen_type_ind('job')]
        for ri, jr in enumerate(job_records):
            siteid_ceid_pairs.append((jr['SiteID'], jr['SubmitHostID']))
        siteid_ceid_pairs = list(set(siteid_ceid_pairs))

        n_pairs = len(siteid_ceid_pairs)

        spec_records = linked_records[self._gen_type_ind('spec')]
        spec_records = spec_records[:n_pairs]

        # - For those spec_records assign a unique SiteID, CEID to each from the list
        # - Using spec as the master, set the ServiceLevelType, ServiceLevel of job records
        #   with matching SiteID, SubmitHostID

        #i = 0
        for sr, pair in zip(spec_records, siteid_ceid_pairs):
            sr['SiteID'], sr['CEID'] = pair

            for jr in job_records: # TODO better algorithm
                jr_pair = jr['SiteID'], jr['SubmitHostID']
                if pair == jr_pair:
                    jr['ServiceLevelType'] = sr['ServiceLevelType']
                    jr['ServiceLevel'] = sr['ServiceLevel']
                else:
                    continue

        # Apply timing logic 

        # spec start < event end or spec start is null, and spec stop > event end
        for sr in spec_records:
            sr['StopTime'] = 'NULL' # NULL
            sr['StartTime'] = datetime.datetime.utcfromtimestamp(0).strftime('%Y-%m-%d %H:%M:%S')

        # TODO (encourage the following)
        # Blahd validfrom < event endtime
        # blahd validuntil > event endtime

        self._linked_records[self._gen_type_ind('spec')] = spec_records

        return self._linked_records


def get_random_int(start=1, end=1000000):
    """Get an random integer between start and end inclusive."""
    x = random.random()
    i = int(x*(end + 1 - start) + start)
    return i


def get_random_float():
    """Get a random float."""
    x = random.random()
    return x * 1000


def get_random_string(strings):
    """Get one of a list of strings at random."""
    x = random.random()
    i = int(x * len(strings))
    return strings[i]


def get_prefix(i):
    prefix = ''
    letters = 'abcdefghij'
    no_str = str(i)
    for number in no_str:
        prefix += letters[int(number)]
        return prefix

def mix_enums(enums):
    # Balance mix correct enums with bad strings
    samples = [get_random_string(sample_strings) for i in range(len(enums))]
    enum_mix = enums + samples
    return get_random_string(enum_mix)

def utc_to_timestamp(utc):
    return datetime.datetime.strptime(utc, '%Y-%m-%d %H:%M:%S').timestamp()

def timestamp_to_utc(timestamp):
    return datetime.datetime.utcfromtimestamp(timestamp).strftime('%Y-%m-%d %H:%M:%S')


allowed_args = [
    "jobs", "summaries", "accelerator", "acceleratorsummary", "event", "blahd", "spec", "join-job-records"
]


def usage():
    """Print a usage message."""
    print("Usage: " + sys.argv[0] + \
        f""" [-r <recs-per-msg> -m <no-msgs> -d <directory> -f <format>] [jobs|summaries|...]

         Defaults: recs-per-msg: 1000
                   no-msgs:      100
                   directory:    ./job-msgs, ./sum-msgs, ...
                   format:       apel, json, csv

        Allowed record types: {allowed_args}
        """)


if __name__ == '__main__':
    """Parse the command-line arguments and create the appropriate type of
    message."""

    recs_per_msg = None
    no_msgs = None
    msg_dir = None
    msg_fmt = None

    opts = None
    args = None

    try:
        opts, args = getopt.getopt(sys.argv[1:], "r:m:d:f:")

    except getopt.GetoptError as e:
        print("Invalid options.")
        usage()
        sys.exit()

    try:
        for o, a in opts:
            if o == "-r":
                recs_per_msg = int(a)
            elif o == "-m":
                no_msgs = int(a)
            elif o == "-d":
                msg_dir = a
            elif o == "-f":
                msg_fmt = a
    except ValueError:
        print("Invalid options.")
        usage()
        sys.exit()

    if "jobs" in args:
        jrg = JobRecordGenerator(recs_per_msg, no_msgs, msg_dir)
        jrg.write_messages(msg_fmt)
    elif "summaries" in args:
        srg = SummaryRecordGenerator(recs_per_msg, no_msgs, msg_dir)
        srg.write_messages(msg_fmt)
    elif "accelerator" in args:
        grg = AcceleratorRecordGenerator(recs_per_msg, no_msgs, msg_dir)
        grg.write_messages(msg_fmt)
    elif "acceleratorsummary" in args:
        gsg = AcceleratorSummaryGenerator(recs_per_msg, no_msgs, msg_dir)
        gsg.write_messages(msg_fmt)
    elif "blahd" in args:
        brg = BlahdRecordGenerator(recs_per_msg, no_msgs, msg_dir)
        brg.write_messages(msg_fmt)
    elif "event" in args:
        erg = EventRecordGenerator(recs_per_msg, no_msgs, msg_dir)
        erg.write_messages(msg_fmt)
    elif "spec" in args:
        srg = SpecRecordGenerator(recs_per_msg, no_msgs, msg_dir)
        srg.write_messages(msg_fmt)
    elif "join-job-records" in args:
        jjrg = JoinJobRecordsGenerator(recs_per_msg, no_msgs, msg_dir)
        jjrg.write_messages(msg_fmt)
    else:
        print(f"Argument must be one of: {', '.join(allowed_args)}.")
        usage()
        sys.exit()
