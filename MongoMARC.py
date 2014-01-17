#-------------------------------------------------------------------------------
# Name:        MongoMARC
# Purpose:     MongoMARC is a wrapper around a MARC record collection ingested
#              into a MongoDB for use in the Catalog Pull Platform
#
# Author:      Jeremy Nelson
#
# Created:     17/01/2014
# Copyright:   (c) Jeremy Nelson 2014
# Licence:     Apache2
#-------------------------------------------------------------------------------


import datetime
import json
import pymarc
import pymongo
import sys


def insert_marc(**kwargs):
    marc_filepath = kwargs.get('marc_filepath')
    mongo_client = kwargs.get('mongo_client',
                              pymongo.MongoClient('mongodb://localhost:27017'))
    marc_db = kwargs.get('marc_db',
                         mongo_client.marc)
    marc_collection = marc_db.marc_records
    marc_reader = pymarc.MARCReader(open(marc_filepath), to_unicode=True)
    start_time = datetime.datetime.utcnow()
    print("Starting MARC load into MongoDB at {0}".format(
    start_time.isoformat()))
    total = None
    for i, record in enumerate(marc_reader):
        if not i%100:
            sys.stderr.write(".")
        if not i%1000:
            sys.stderr.write(str(i))
        json_record = json.loads(record.as_json())
        marc_collection.insert(json_record)
        total = i
    end_time = datetime.datetime.utcnow()
    print("Finished MARC load into MongoDB at {0}".format(
    end_time.isoformat()))
    duration = end_time - start_time
    print("Total hours={0} minutes={1} for {2} records".format(
    duration.total_seconds()/3600.0,
    duration.total_seconds()/60.0,
    total))

def main():
    pass

if __name__ == '__main__':
    main()