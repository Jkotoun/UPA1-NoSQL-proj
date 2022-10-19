from concurrent.futures import process
from fileinput import filename
import xmltodict
from pymongo import MongoClient, InsertOne
import os
import datetime

def get_database():

    CONNECTION_STRING = "mongodb://localhost:27017"
    client = MongoClient(CONNECTION_STRING)
    return client['upadb']


def decode_bitmap(startdate: datetime, bitmap: str,  path_canceled_bit: str, path_not_canceled_bit: str):

    def push_dates_interval(startdate: datetime, enddate: datetime, dates_list: list):
        dates_list.append({
            'startdate': startdate,
            'enddate': enddate
        })
        return dates_list

    canceled_days_intervals = []
    canceled_bits_sequence = 0

    for bit in bitmap:

        if bit == path_canceled_bit:
            canceled_bits_sequence += 1

        elif bit == path_not_canceled_bit:
            startdate += datetime.timedelta(days=1)
            # canceled bits seqence before not-canceled bit
            if canceled_bits_sequence > 0:
                interval_startdate = startdate
                startdate += datetime.timedelta(days=canceled_bits_sequence)
                canceled_days_intervals = push_dates_interval(
                    interval_startdate, startdate, canceled_days_intervals)
                canceled_bits_sequence = 0

    # bitmaps ends with canceled bits sequence
    if not canceled_bits_sequence == 0:
        interval_startdate = startdate
        startdate += datetime.timedelta(days=canceled_bits_sequence)
        canceled_days_intervals = push_dates_interval(
            interval_startdate, startdate, canceled_days_intervals)

    return canceled_days_intervals


def upsert_train_paths(mongodb_instance, cismessages_dir):
    upserts = []
    train_paths_collection = mongodb_instance["trains_timetable"]
    # cis messages
    for xmlfilename in os.listdir(cismessages_dir):
        if xmlfilename.endswith('.xml'):
            if train_paths_collection.count_documents({'_id': xmlfilename}) > 0:
                continue

            with open(f'{cismessages_dir}/{xmlfilename}', encoding="utf-8") as xml_file:
                train_path = xmltodict.parse(xml_file.read())

                planned_calendar = train_path['CZPTTCISMessage']['CZPTTInformation']['PlannedCalendar']
                planned_calendar_startdate = datetime.datetime.strptime(
                    planned_calendar['ValidityPeriod']['StartDateTime'], '%Y-%m-%dT%H:%M:%S')
                planned_calendar_bitmap = planned_calendar['BitmapDays']
                train_path['canceled'] = decode_bitmap(
                    planned_calendar_startdate, planned_calendar_bitmap, path_canceled_bit='0', path_not_canceled_bit='1')
                train_path['_id'] = xmlfilename
                upserts.append(InsertOne(train_path))
    if upserts:
        train_paths_collection.bulk_write(upserts)

def process_canceled_messages(mongodb_instance, canceledmessages_dir):
    
    train_paths_collection = mongodb_instance["trains_timetable"]
    applied_cancel_messages_collection = mongodb_instance["applied_cancel_messages"]

    for xmlfile in os.listdir(canceledmessages_dir):
        if xmlfile.endswith('.xml'):
            if(applied_cancel_messages_collection.count_documents({'cancelMessageFileName': xmlfile}) == 0):
                with open(f'{canceledmessages_dir}/{xmlfile}', encoding="utf-8") as xml_file:

                    canceled_message = xmltodict.parse(xml_file.read())
                    cancelation_startdate = datetime.datetime.strptime( canceled_message['CZCanceledPTTMessage']['PlannedCalendar']['ValidityPeriod']['StartDateTime'], '%Y-%m-%dT%H:%M:%S')
                    cancelation_bitmap = canceled_message['CZCanceledPTTMessage']['PlannedCalendar']['BitmapDays']
                    cancelation_intervals = decode_bitmap(
                        cancelation_startdate, cancelation_bitmap, path_canceled_bit='1', path_not_canceled_bit='0')
                    #add cancellation interval to path with given transport identifiers
                    train_paths_collection.find_one_and_update({"CZPTTCISMessage.Identifiers.PlannedTransportIdentifiers": canceled_message["CZCanceledPTTMessage"]["PlannedTransportIdentifiers"]}, {"$push": {
                        "canceled": {
                            "$each": cancelation_intervals
                        }
                    }})
                    applied_cancel_messages_collection.insert_one(
                        {'cancelMessageFileName': xmlfile})

def db_upsert_data(mongodb_instance, cismessages_dir, canceledmessages_dir):
    upsert_train_paths(mongodb_instance, cismessages_dir)
    process_canceled_messages(mongodb_instance, canceledmessages_dir)



if __name__ == "__main__":
    mongodb_instance = get_database()
    cismessages_dir = './archives'
    canceledmessages_dir = './archives/canceled'
    db_upsert_data(mongodb_instance, cismessages_dir, canceledmessages_dir)
