from fileinput import filename
import xmltodict
from pymongo import MongoClient, UpdateOne
import os
import datetime


def get_database():

    CONNECTION_STRING = "mongodb://localhost:27017"

    # Create a connection using MongoClient. You can import MongoClient or use pymongo.MongoClient
    client = MongoClient(CONNECTION_STRING)

    # Create the database for our example (we will use the same database throughout the tutorial
    return client['upadb']


def decode_bitmap(startdate :datetime, bitmap: str,  path_canceled_bit: str, path_not_canceled_bit :str):

    def push_dates_interval(startdate: datetime, enddate:datetime, dates_list: list):
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
            startdate+=datetime.timedelta(days=1)
            #canceled bits seqence before not-canceled bit
            if canceled_bits_sequence > 0:
                interval_startdate = startdate
                startdate += datetime.timedelta(days=canceled_bits_sequence)
                canceled_days_intervals = push_dates_interval(interval_startdate, startdate, canceled_days_intervals)
                canceled_bits_sequence=0

    #bitmaps ends with canceled bits sequence
    if not canceled_bits_sequence == 0:
        interval_startdate = startdate
        startdate += datetime.timedelta(days=canceled_bits_sequence)
        canceled_days_intervals = push_dates_interval(interval_startdate, startdate, canceled_days_intervals)

    return canceled_days_intervals



if __name__ == "__main__":
   
    mongodb_instance = get_database()
    train_paths_collection = mongodb_instance["trains_timetable"]
    applied_cancel_messages_collection = mongodb_instance["applied_cancel_messages"]
    cismessages_dirname = './archives'
    canceledmessages_dirname = './archives/canceled'


    upserts = []

    #cis messages 
    for xmlfile in os.listdir(cismessages_dirname):
        if xmlfile.endswith('.xml'):
            # if train_paths_collection.count_documents({'file_name': xmlfile}) > 0:
            #     continue

            with open(f'{cismessages_dirname}/{xmlfile}', encoding="utf-8") as xml_file:
                train_path = xmltodict.parse(xml_file.read())

                planned_calendar = train_path['CZPTTCISMessage']['CZPTTInformation']['PlannedCalendar']
                planned_calendar_startdate = datetime.datetime.strptime(planned_calendar['ValidityPeriod']['StartDateTime'], '%Y-%m-%dT%H:%M:%S')
                planned_calendar_bitmap = planned_calendar['BitmapDays']
                train_path['canceled'] = decode_bitmap(planned_calendar_startdate, planned_calendar_bitmap, path_canceled_bit='0', path_not_canceled_bit='1')
                upserts.append(
                    UpdateOne(
                        {'_id': xmlfile},
                        {'$setOnInsert': train_path},
                        upsert=True
                    )
            )


        else:
            continue

    result = train_paths_collection.bulk_write(upserts)

    #canceled messaged
    for xmlfile in os.listdir(canceledmessages_dirname):
        if xmlfile.endswith('.xml'):
             if(applied_cancel_messages_collection.count_documents({'cancelMessageFileName': xmlfile}) == 0):
                with open(f'{canceledmessages_dirname}/{xmlfile}', encoding="utf-8") as xml_file:
                    
                    canceled_message = xmltodict.parse(xml_file.read())
                    
                    cancelation_startdate =datetime.datetime.strptime( canceled_message['CZCanceledPTTMessage']['PlannedCalendar']['ValidityPeriod']['StartDateTime'], '%Y-%m-%dT%H:%M:%S')
                    cancelation_bitmap = canceled_message['CZCanceledPTTMessage']['PlannedCalendar']['BitmapDays']
                    cancelation_intervals = decode_bitmap(cancelation_startdate, cancelation_bitmap, path_canceled_bit='1', path_not_canceled_bit='0')
                    
                    canceleddoc = train_paths_collection.find_one_and_update({"CZPTTCISMessage.Identifiers.PlannedTransportIdentifiers": canceled_message["CZCanceledPTTMessage"]["PlannedTransportIdentifiers"]}, {"$push": {
                        "canceled":{
                            "$each": cancelation_intervals
                        }
                    }})
                    applied_cancel_messages_collection.insert_one({'cancelMessageFileName': xmlfile})
                
                
             




# train_paths_collection.insert_many(train_paths_to_insert)


# with open("data.json", "w", encoding='utf-8') as json_file:
#         json_file.write(json_data)
