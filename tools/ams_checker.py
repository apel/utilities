"""Script to query AMS subscriptions and show the backlog of messages."""

# References used in the creation of this script.
# https://realpython.com/python-string-formatting/
# https://stackoverflow.com/a/12965254/1442342

from __future__ import print_function

import json
import urllib

# An admin token for the AMS project needs to be provided.
TOKEN = ''
URL_TEMPLATE = ('https://msg.argo.grnet.gr/v1/projects/accounting/'
                'subscriptions/{sub}:offsets?key={token}')
TYPES = ('grid', 'cloud', 'storage')
subs = []

print("Subscription     \tBacklog")
print("-"*31)
for type in TYPES:
    for service in ('repository', 'portal'):
        sub = service + '-' + type
        subs.append(sub)
        if service == 'portal':
            sub = service + '-' + type + '-preprod'
            subs.append(sub)

for type in TYPES[0:2]:
    sub = 'IRIS-' + type + '-APEL'
    subs.append(sub)

for sub in subs:
    url = URL_TEMPLATE.format(sub=sub, token=TOKEN)
    response = urllib.urlopen(url)
    data = json.loads(response.read())
    print(sub, data['max'] - data['current'], sep='     \t')
