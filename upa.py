import argparse
import datetime
import os
import file_sync
from datetime import datetime, timedelta

import dateutil.parser
import xmltodict
from pymongo import CursorType, UpdateOne, MongoClient


def get_database(mongo_url):

    CONNECTION_STRING = "mongodb://"+ mongo_url
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
            startdate += timedelta(days=1)
            # canceled bits seqence before not-canceled bit
            if canceled_bits_sequence > 0:
                interval_startdate = startdate
                startdate += timedelta(days=canceled_bits_sequence)
                canceled_days_intervals = push_dates_interval(
                    interval_startdate, startdate, canceled_days_intervals)
                canceled_bits_sequence = 0

    # bitmaps ends with canceled bits sequence
    if not canceled_bits_sequence == 0:
        interval_startdate = startdate
        startdate += timedelta(days=canceled_bits_sequence)
        canceled_days_intervals = push_dates_interval(
            interval_startdate, startdate, canceled_days_intervals)

    return canceled_days_intervals


def upsert_train_paths(mongodb_instance, cismessages_dir):
    upserts = []
    train_paths_collection = mongodb_instance["trains_timetable"]
    # cis messages
    for xmlfilename in os.listdir(cismessages_dir):
        if xmlfilename.endswith('.xml'):

            with open(f'{cismessages_dir}/{xmlfilename}', encoding="utf-8") as xml_file:
                train_path = xmltodict.parse(xml_file.read())
                planned_calendar = train_path['CZPTTCISMessage']['CZPTTInformation']['PlannedCalendar']
                planned_calendar_startdate = datetime.strptime(
                    planned_calendar['ValidityPeriod']['StartDateTime'], '%Y-%m-%dT%H:%M:%S')
                planned_calendar_bitmap = planned_calendar['BitmapDays']
                train_path['canceled'] = decode_bitmap(
                    planned_calendar_startdate, planned_calendar_bitmap, path_canceled_bit='0', path_not_canceled_bit='1')
                upserts.append( UpdateOne(
                        {'_id': xmlfilename},
                        {'$set': train_path,
                        '$setOnInsert': {'_id': xmlfilename}},
                        upsert=True
                    ))
    if upserts:
        train_paths_collection.bulk_write(upserts)

def process_canceled_messages(mongodb_instance, canceledmessages_dir):
    
    train_paths_collection = mongodb_instance["trains_timetable"]
    applied_cancel_messages_collection = mongodb_instance["applied_cancel_messages"]

    for xmlfile in os.listdir(canceledmessages_dir):
        if xmlfile.endswith('.xml'):
            with open(f'{canceledmessages_dir}/{xmlfile}', encoding="utf-8") as xml_file:
                canceled_message = xmltodict.parse(xml_file.read())
                cancelation_startdate = datetime.strptime( canceled_message['CZCanceledPTTMessage']['PlannedCalendar']['ValidityPeriod']['StartDateTime'], '%Y-%m-%dT%H:%M:%S')
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


def filter_data(collection, from_station:str, to_station:str, datetime_obj:datetime):
    timestring = datetime_obj.strftime("%H:%M:%S")
    date = datetime(datetime_obj.year, datetime_obj.month, datetime_obj.day)
    data= collection.aggregate([
    {
        '$match': {
            '$and': [
                {
                    'CZPTTCISMessage.CZPTTInformation.CZPTTLocation': {
                        '$elemMatch': {
                            'Location.PrimaryLocationName': from_station, 
                            'TrainActivity.TrainActivityType': '0001', 
                            'TimingAtLocation.Timing.@TimingQualifierCode': 'ALD', 
                            'TimingAtLocation.Timing.Time': {
                                '$gte': timestring
                            }
                        }
                    }
                }, {
                    'CZPTTCISMessage.CZPTTInformation.CZPTTLocation': {
                        '$elemMatch': {
                            'Location.PrimaryLocationName': to_station, 
                            'TrainActivity.TrainActivityType': '0001'
                        }
                    }
                }, {
                    'canceled': {
                        '$not': {
                            '$elemMatch': {
                                'startdate': {
                                    '$lte': date
                                }, 
                                'enddate': {
                                    '$gte': date
                                }
                            }
                        }
                    }
                }
            ]
        }
    }, {
        '$project': {
            'stops': '$CZPTTCISMessage.CZPTTInformation.CZPTTLocation.Location.PrimaryLocationName', 
            'times': '$CZPTTCISMessage.CZPTTInformation.CZPTTLocation.TimingAtLocation.Timing', 
            'idx_start': {
                '$indexOfArray': [
                    '$CZPTTCISMessage.CZPTTInformation.CZPTTLocation.Location.PrimaryLocationName', from_station
                ]
            }, 
            'idx_end': {
                '$indexOfArray': [
                    '$CZPTTCISMessage.CZPTTInformation.CZPTTLocation.Location.PrimaryLocationName', to_station
                ]
            }
        }
    }, {
        '$match': {
            '$expr': {
                '$lt': [
                    '$idx_start', '$idx_end'
                ]
            }
        }
    }
])
    return data

def print_data(cursor:CursorType):
    def format_time_string(timestring):
       return dateutil.parser.parse(timestring).strftime("%H:%M:%S")

    for train in cursor:
        print(f"Train id: {train['_id']}")
        stops = train["stops"]
        times = train["times"]
        for stop,time in zip(stops, times):
            time_str=""
            if isinstance(time, list):
                time_str = f"{format_time_string(time[0]['Time'])} - {format_time_string(time[1]['Time'])}"
            else:
                time_str = format_time_string(time["Time"])+"\t"
            print(f"{time_str}\t{stop}")
        print("------------------------------------------------------------")





if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument('odkud', type=str, help="název stanice odkud")
    parser.add_argument('kam', type=str, help="název stanice kam")
    parser.add_argument('kdy', type=str, help="datetime string pro čas nástupu ve stanici odkud (v libovolném rozumném formátu)")
    parser.add_argument('--no_upsert', action="store_true", help="data se rovnou vyfiltrují z db.")
    parser.add_argument('--no_download', action="store_true", help="data se nebudou stahovat z ftp serveru.")
    parser.add_argument('--mongodb_url', default="localhost:27017", help="url mongodb serveru (default: localhost:27017)")

    args = parser.parse_args()
    kdy = dateutil.parser.parse(args.kdy)

    mongodb_instance = get_database(args.mongodb_url)

    if not args.no_upsert:
        cismessages_dir = './archives'
        canceledmessages_dir = './archives/canceled'
        db_upsert_data(mongodb_instance, cismessages_dir, canceledmessages_dir)

    if not args.no_download:
        fs = file_sync.fileSynchronizator()
        fs.get_all_xmls()

    cursor = filter_data(mongodb_instance["trains_timetable"], args.odkud, args.kam, kdy)
    print_data(cursor)
