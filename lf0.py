import math
import dateutil.parser
import datetime
import time
import os
import logging
import boto3
import json
import re
import datetime

logger = logging.getLogger()
logger.setLevel(logging.DEBUG)
# Get the service resource
sqs = boto3.resource('sqs')

# Get the queue
queue = sqs.get_queue_by_name(QueueName='orderfoodqueue.fifo')


""" --- Helpers to build responses which match the structure of the necessary dialog actions --- """


def get_slots(intent_request):
    return intent_request['currentIntent']['slots']


def elicit_slot(session_attributes, intent_name, slots, slot_to_elicit, message):
    return {
        'sessionAttributes': session_attributes,
        'dialogAction': {
            'type': 'ElicitSlot',
            'intentName': intent_name,
            'slots': slots,
            'slotToElicit': slot_to_elicit,
            'message': message
        }
    }


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


def delegate(session_attributes, slots):
    return {
        'sessionAttributes': session_attributes,
        'dialogAction': {
            'type': 'Delegate',
            'slots': slots
        }
    }


""" --- Helper Functions --- """

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


def build_validation_result(is_valid, violated_slot, message_content):
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


def validate_order_food(cuisine, location, dining_time, num_people, email):
    cuisine_types = ['american', 'chinese', 'indian', 'italian']
    location_list = ['manhattan', 'brooklyn', 'new jersey']

    if cuisine is not None and cuisine.lower() not in cuisine_types:
        return build_validation_result(False,
                                       'Cuisine' ,
                                       'We do not know of any {} foods, would you like a different type of cuisine?'.format(cuisine))
                                       
    if location is not None and location.lower() not in location_list:
        return build_validation_result(False,
                                        'Location' ,
                                        'We do not support this location. Please enter an area within NYC')
                                        
    if dining_time is not None:
        now = datetime.datetime.now()
        if len(dining_time) != 5:
            # Not a valid time; use a prompt defined on the build-time model.
            return build_validation_result(False, 'DiningTime', 'Please enter a valid time (AM/PM)')

        hour, minute = dining_time.split(':')
        hour = parse_int(hour)
        minute = parse_int(minute)
        if math.isnan(hour) or math.isnan(minute):
            # Not a valid time; use a prompt defined on the build-time model.
            return build_validation_result(False, 'DiningTime', 'Please enter a valid time (AM/PM)')
        requestedtime = datetime.datetime.strptime(dining_time, "%H:%M")
        print(requestedtime.time())
        print(now.time())
        if requestedtime.time() < now.time():
            return build_validation_result(False, 'DiningTime', 'Please enter a time in the future.')

    if num_people is not None:
        numberPeople = parse_int(num_people)
        if math.isnan(numberPeople): 
            return build_validation_result(False,
                                        'NumberPeople',
                                        'Please enter a valid amount of guests.')
        if numberPeople < 0 or numberPeople > 20:
            return build_validation_result(False, 'NumberPeople', 'Please enter an amount between 0 and 20.')
    
    if email is not None:
        regex = r'\b[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Z|a-z]{2,}\b'
        # email = parse_int(phone_number)
        # if math.isnan(phoneNumber): 
        if not re.fullmatch(regex, email):
            return build_validation_result(False, 'Email', 'Please enter a valid e-mail.')
        # if hour < 8 or hour > 22:
            # Outside of business hours
            # return build_validation_result(False, 'DiningTime', 'Our business hours are from ten a m. to five p m. Can you specify a time during this range?')

    return build_validation_result(True, None, None)


""" --- Functions that control the bot's behavior --- """


def order_food(intent_request):
    """
    Performs dialog management and fulfillment for ordering flowers.
    Beyond fulfillment, the implementation of this intent demonstrates the use of the elicitSlot dialog action
    in slot validation and re-prompting.
    """

    cuisine_type = get_slots(intent_request)["Cuisine"]
    location = get_slots(intent_request)["Location"]
    dining_time = get_slots(intent_request)["DiningTime"]
    num_people = get_slots(intent_request)["NumberPeople"]
    email = get_slots(intent_request)["Email"]
    
    source = intent_request['invocationSource']

    if source == 'DialogCodeHook':
        # Perform basic validation on the supplied input slots.
        # Use the elicitSlot dialog action to re-prompt for the first violation detected.
        slots = get_slots(intent_request)

        validation_result = validate_order_food(cuisine_type, location, dining_time, num_people, email)
        if not validation_result['isValid']:
            slots[validation_result['violatedSlot']] = None
            return elicit_slot(intent_request['sessionAttributes'],
                               intent_request['currentIntent']['name'],
                               slots,
                               validation_result['violatedSlot'],
                               validation_result['message'])
        
        # Pass the price of the flowers back through session attributes to be used in various prompts defined
        # on the bot model.
        output_session_attributes = intent_request['sessionAttributes'] if intent_request['sessionAttributes'] is not None else {}
        #if cuisine_type is not None:
        #    output_session_attributes['Price'] = len(cuisine_type) * 5  # Elegant pricing model
        
        #output_session_attributes['Cuisine'] = len(cuisine_type) * 5 
        #output_session_attributes['Location'] = location  
        #output_session_attributes['DiningTime'] = dining_time  
        #output_session_attributes['NumberPeople'] = num_people  
        #output_session_attributes['PhoneNumber'] = phone_number  

        return delegate(output_session_attributes, get_slots(intent_request))

    #send messages to sqs
    slots = get_slots(intent_request)
    sendToQueue(intent_request)

    # Order the flowers, and rely on the goodbye message of the bot to define the message to the end user.
    # In a real bot, this would likely involve a call to a backend service.
    return close(intent_request['sessionAttributes'],
                 'Fulfilled',
                 {'contentType': 'PlainText',
                  'content': 'Great! You are all set. I will send you my recommendations soon to {}. Expect them shortly.'.format(email) })

    #'Thanks, your order for {} has been placed and will be ready for pickup at {} in {}'.format(cuisine_type, dining_time, location)
""" --- Intents --- """


def dispatch(intent_request):
    """
    Called when the user specifies an intent for this bot.
    """

    logger.debug('dispatch userId={}, intentName={}'.format(intent_request['userId'], intent_request['currentIntent']['name']))

    intent_name = intent_request['currentIntent']['name']

    # Dispatch to your bot's intent handlers
    if intent_name == 'DiningSuggestionsIntent':
        return order_food(intent_request)

    raise Exception('Intent with name ' + intent_name + ' not supported')


""" --- Main handler --- """


def lambda_handler(event, context):
    """
    Route the incoming request based on intent.
    The JSON body of the request is provided in the event slot.
    """
    # By default, treat the user request as coming from the America/New_York time zone.
    os.environ['TZ'] = 'America/New_York'
    time.tzset()
    logger.debug('event.bot.name={}'.format(event['bot']['name']))

    return dispatch(event)

def sendToQueue(event):
    print("MESSAGE: ", event)
    # Create a new message
    response = queue.send_message(MessageBody="Message from LF1", 
     MessageAttributes={
                "Location": {
                    "StringValue": str(get_slots(event)["Location"]),
                    "DataType": "String"
                },
                "Cuisine": {
                    "StringValue": str(get_slots(event)["Cuisine"]),
                    "DataType": "String"
                },
                "Time" : {
                    "StringValue": str(get_slots(event)["DiningTime"]),
                    "DataType": "String"
                },
                "NumPeople" : {
                    "StringValue": str(get_slots(event)["NumberPeople"]),
                    "DataType": "String"
                },
                "Email" : {
                    "StringValue": str(get_slots(event)["Email"]),
                    "DataType": "String"
                }
         },
         MessageGroupId = 'Dining'
     )
