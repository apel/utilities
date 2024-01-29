#!/usr/bin/env python
# coding: utf-8
"""Module for loading json messages into a dirq directory

This script can feed either a single message file or a tar
file of messages into a queue.

Author: Connor Pettitt
Contact: connor.pettitt@stfc.ac.uk
"""

import argparse
import json
import tarfile

from dirq import queue

schema = {"body": "string", "signer": "string",
          "empaid": "string?", "error": "string?"}

default_path = './queue/incoming'

def main(messages_tar=None, path='./queue'):
    """ DOC
    messages_tar: filename
        Existing filename to tar archive containing files with message bodies.

    path: pathname
        Pathname to directory (created if non-existant) where dirq will place messages.
    """
    message_strings = []

    if 'tar' in messages_tar:
        try:
            with tarfile.open(name=messages_tar, mode='r', fileobj=None, bufsize=10240) as tar:
                for file in tar:
                    message_strings.append(tar.extractfile(file).read())
        except:
            print(f'Not a valid tar archive: {messages_tar}')
            exit(1)
    else: # Try to load as text file
        message_strings = [open(messages_tar, 'r').read()]

    message_bodies = []

    print('Loading json strings.', end='')
    for mi, mstr in enumerate(message_strings):        
        # TODO check message type
        # TODO load apel format messages

        #try:
        body = json.loads(mstr)
        message_bodies.append(body)
        #except:
            #print('Failed to load message:', mstr)
            #print('')


        if mi % 100 == 0: print('.', end='')
    print('')
        
    Q = queue.Queue(path, schema=schema)

    print('Pushing messages into queue.', end='')
    for mi, body in enumerate(message_bodies):
        body = json.dumps(body).replace("'",'"')
        message = {'body':body, 'signer':'test signer', 'empaid':'', 'error':''}

        #try:
        Q.add(message)
        #except:
            #print(f'Failed to add message to queue: {message}')

        if mi % 100 == 0: print('.', end='')
    print('')


if __name__ == '__main__':
    import os

    parser = argparse.ArgumentParser(description='Get input and output pathnames.')

    parser.add_argument('messages_tar', default=None, help='Supply a filename for input messages')
    parser.add_argument('-p/--path', dest='path', default='./queue/incoming',
                        help=f'Supply a path for Queue output (default: {default_path}).',
                        required=False)

    args = parser.parse_args()

    messages_tar = args.messages_tar

    if not os.path.exists(messages_tar):
        print(f'Tried to find file: {messages_tar} but it appears not to exist.')
        print('Please use flag --messages FILENAME with either a text file or a tar archive.')
        exit(1)

    try:
        open(messages_tar, 'r')
    except:
        print(f'Tried to read: {messages_tar} but failed. ')
        print(f'Please check that path is a file and has the correct permissions.')
        exit(1)

    if args.path == default_path:
        print(f'Defaulting to path: {default_path} for output queue.')

    if not os.path.exists(args.path):
        os.makedirs(args.path)

    path = args.path
    messages_tar = args.messages_tar

    main(messages_tar=messages_tar, path=path)
