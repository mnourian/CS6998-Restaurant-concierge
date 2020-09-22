# LF1

import boto3
import json
import datetime
import time
import os
import math
import dateutil.parser
import time
import os
import logging

logger = logging.getLogger()
logger.setLevel(logging.DEBUG)
SQS = boto3.client("sqs")
QUEUE_URL = 'https://sqs.us-east-1.amazonaws.com/999183881955/DiningConciergeSQS'


logger = logging.getLogger()
logger.setLevel(logging.DEBUG)
SQS = boto3.client("sqs")
QUEUE_URL = 'https://sqs.us-east-1.amazonaws.com/999183881955/DiningConciergeSQS'
    
def record(intent_request):
    logger.debug("Recording with event %s", intent_request)
    location = get_slot_val(get_slots(intent_request)["location"]) # [ERROR] TypeError: list indices must be integers or slices, not str Traceback (most recent call last):   File "/var/task/lambda_function.py", line 343, in lambda_handler     return dispatch(event,context)   File "/var/task/lambda_function.py", line 319, in dispatch     return diningSuggestions(intent_request,context)   File "/var/task/lambda_function.py", line 234, in diningSuggestions     location = get_slot_val(get_slots(intent_request)["location"])   File "/var/task/lambda_function.py", line 227, in get_slot_val     return ['value']['originalValue']
    cuisine =  get_slot_val(get_slots(intent_request)["cuisine"])
    date = get_slot_val_date(get_slots(intent_request)["date"])
    time = get_slot_val(get_slots(intent_request)["time"])
    numberOfPeople = get_slot_val(get_slots(intent_request)["numberOfPeople"])
    phoneNumber = get_slot_val(get_slots(intent_request)["phoneNumber"])
    email = get_slot_val(get_slots(intent_request)["email"])
    
    print('XYZ - email:', email)
    print('XYZ - date:', date)

    try:
        resp = SQS.send_message(
            QueueUrl=QUEUE_URL, 
            MessageBody="Dining Concierge message from LF1 ",
            MessageAttributes={
                "Location": {
                    "StringValue": str(location),
                    "DataType": "String"
                },
                "Cuisine": {
                    "StringValue": str(cuisine),
                    "DataType": "String"
                },
                "Date" : {
                    "StringValue": str(date),
                    "DataType": "String"
                },
                "Time" : {
                    "StringValue": str(time),
                    "DataType": "String"
                },
                "NumPeople" : {
                    "StringValue": str(numberOfPeople),
                    "DataType": "String"
                },
                "PhoneNum" : {
                    "StringValue": str(phoneNumber),
                    "DataType": "String"
                }, "Email" : {
                    "StringValue": str(email),
                    "DataType": "String"
                }
            }
        )
        print('XYZ- record - resp', resp)
    except Exception as e:
        raise Exception("Could not record link! %s" % e)

def get_slots(intent_request):
    return intent_request['sessionState']['intent']['slots']


def close(session_attributes, fulfillment_state, message):
    response = {
        'sessionAttributes': session_attributes,
        'dialogAction': {
            'type': 'Close',
            'fulfillmentState': fulfillment_state,
            'message': message
        }
    }

    return response


def delegate(slots):
    print('XYZ - delegate is called')
    print('XYZ - slots:', slots)
    return {
        "sessionState": {
            "dialogAction": {
                "type": "Delegate"
                },
                "intent": {
                    'name': 'DiningSuggestionsIntent',
                    'slots': slots
                }
            }
        }

def isvalid_date(date):
    try:
        dateutil.parser.parse(date)
        return True
    except ValueError:
        return False


def parse_int(n):
    try:
        return int(n)
    except ValueError:
        return float('nan')

def validate_input(is_valid, violated_slot, message_content):
    if message_content is None:
        return {
            "isValid": is_valid,
            "violatedSlot": violated_slot,
        }

    return {
        'isValid': is_valid,
        'violatedSlot': violated_slot,
        'message': {'contentType': 'PlainText', 'content': message_content}
    }

def validate_location(location):
    locations = ['manhattan', 'new york']
    print('XYZ - input - location', location)
    if location is not None and location.lower() not in locations:
        return validate_input(False,
                                       'location',
                                       'We do not have suggestions for {}, would you like suggestions for a differenet location?  '
                                       'Our most popular location is Manhattan '.format(location))

def validate_cusine(cuisine):
    cuisines = ['chinese', 'indian', 'italian', 'japanese', 'mexican', 'thai', 'korean', 'arab', 'american']
    if cuisine is not None and cuisine.lower() not in cuisines:
        return validate_input(False,
                                       'cuisine',
                                       'We do not have suggestions for {}, would you like suggestions for a differenet cuisine ?  '
                                       'Our most popular Cuisine is Indian '.format(cuisine))

def validate_time(time):
    if time is not None:
        if len(time) != 5:
            return validate_input(False, 'DiningTime', None)

        hour, minute = time.split(':')
        hour = parse_int(hour)
        minute = parse_int(minute)
        if math.isnan(hour) or math.isnan(minute):
            return validate_input(False, 'time', None)

        if hour < 10 or hour > 24:
            return validate_input(False, 'time', 'Our business hours are from 10 AM. to 11 PM. Can you specify a time during this range?')

def validate_people(numberOfPeople):
    if numberOfPeople is not None and not numberOfPeople.isnumeric():
        return validate_input(False,
                                       'numberOfPeople',
                                       'That does not look like a valid number {}, '
                                       'Could you please repeat?'.format(numberOfPeople))

def validate_phone_number(phoneNumber):
    if phoneNumber is not None and not phoneNumber.isnumeric():
        return validate_input(False,
                                       'phoneNumber',
                                       'That does not look like a valid number {}, '
                                       'Could you please repeat? '.format(phoneNumber))    

def validate_dining_suggestion(location, cuisine, time, date, numberOfPeople, phoneNumber):
    loc_val = validate_location(location)
    if loc_val is not None:
        return loc_val
                                       
    
    cuisine_val = validate_cusine(cuisine)
    if cuisine_val is not None:
        return cuisine_val

    time_val = validate_time(time)
    if time_val is not None:
        return time_val
    

    people_val = validate_people(numberOfPeople)
    if people_val is not None:
        return people_val
    
    phone_number_val = validate_phone_number(phoneNumber)
    if phone_number_val is not None:
        return phone_number_val
    
    return validate_input(True, None, None)


def get_slot_val(slot_val):
    if slot_val is not None:
        return slot_val['value']['originalValue']
    return None
    
def get_slot_val_date(slot_val):
    if slot_val is not None:
        return slot_val['value']['interpretedValue']
    return None


def diningSuggestions(intent_request,context):
    
    # XYZ - input - location {'shape': 'Scalar', 'value': {'originalValue': 'manhattan', 'resolvedValues': ['manhattan'], 'interpretedValue': 'manhattan'}}

    location = get_slot_val(get_slots(intent_request)["location"]) # [ERROR] TypeError: list indices must be integers or slices, not str Traceback (most recent call last):   File "/var/task/lambda_function.py", line 343, in lambda_handler     return dispatch(event,context)   File "/var/task/lambda_function.py", line 319, in dispatch     return diningSuggestions(intent_request,context)   File "/var/task/lambda_function.py", line 234, in diningSuggestions     location = get_slot_val(get_slots(intent_request)["location"])   File "/var/task/lambda_function.py", line 227, in get_slot_val     return ['value']['originalValue']
    cuisine =  get_slot_val(get_slots(intent_request)["cuisine"])
    date = get_slot_val_date(get_slots(intent_request)["date"])
    time = get_slot_val(get_slots(intent_request)["time"])
    numberOfPeople = get_slot_val(get_slots(intent_request)["numberOfPeople"])
    phoneNumber = get_slot_val(get_slots(intent_request)["phoneNumber"])
    source = intent_request['invocationSource']
    slots = get_slots(intent_request)
    intent_name = intent_request['sessionState']['intent']['name']

    if source == 'DialogCodeHook':
        validation_result = validate_dining_suggestion(location, cuisine, time, date, numberOfPeople, phoneNumber)
        if not validation_result['isValid']:
            slots[validation_result['violatedSlot']] = None
            return {
                    "sessionState": {
                        "dialogAction": {
                            "slotToElicit": validation_result['violatedSlot'],
                            "type": "ElicitSlot"
                        },
                        "intent": {
                            "name": intent_name,
                            "slots": slots
                        }
                    }
                }
            
        print('XYZ - called delegate')
        # if all the input are placed and validate, pass and call record
        return delegate(get_slots(intent_request))
    
    print('XYZ - record was called')
    record(intent_request)
    return {
            "sessionState": {
                "dialogAction": {
                    "type": "Close"
                },
                "intent": {
                    "name": intent_name,
                    "slots": slots,
                    "state": "Fulfilled"
                }

            },
            "messages": [
                {
                    "contentType": "PlainText",
                    "content": "Thank you! We are processing your request and you should receive an email shortly!"
                }
            ]
        }

def welcome(intent_request):
    return {
            "sessionState": {
                "dialogAction": {
                    "type": "Close"
                },
                "intent": {
                    "name": 'GreetingIntent',
                    "state": "Fulfilled"
                }

            },
            "messages": [
                {
                    "contentType": "PlainText",
                    "content": "What can I help you with today?"
                }
            ]
        }

def thankYou(intent_request):
    return {
            "sessionState": {
                "dialogAction": {
                    "type": "Close"
                },
                "intent": {
                    "name": 'ThankYouIntent',
                    "state": "Fulfilled"
                }

            },
            "messages": [
                {
                    "contentType": "PlainText",
                    "content": "Thank you and have a great day!"
                }
            ]
        }


def dispatch(intent_request,context):
    print('XYZ - intent_request:', intent_request)

    intent_name = intent_request['sessionState']['intent']['name']
    if intent_name == 'DiningSuggestionsIntent':
        return diningSuggestions(intent_request,context)
    elif intent_name == 'ThankYouIntent':
        return thankYou(intent_request)
    elif intent_name == 'GreetingIntent':
        return welcome(intent_request)

def lambda_handler(event, context):
    os.environ['TZ'] = 'America/New_York'
    time.tzset()
    logger.debug('event.bot.name={}'.format(event['bot']['name']))
    
    print('XXXX - LF1, event:', event)

    return dispatch(event,context)