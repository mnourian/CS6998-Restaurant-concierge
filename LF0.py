# LF0

import json
import boto3


# global values
bot_name = 'DiningConcierge'
bot_id='FWH7DEV6GN'
bot_alias_id='TSTALIASID' # we are test bot alias versian
session_id='testuser'
bot_alias_name = 'DiningConciergeAliasV1'
user_id = 'test'

def send_to_lex_bot(user_text):
    client = boto3.client('lexv2-runtime')
    
    print('LG0: user_text', user_text) # returns the raw text
    
    bot_response = client.recognize_text(
        botId=bot_id, # MODIFY HERE
        botAliasId=bot_alias_id, # MODIFY HERE
        localeId='en_US',
        sessionId='testuser',
        text=user_text)
    
    print('XYZ - user_text:', user_text)
    print('XYZ - bot_response:', bot_response)
    
    
    messages_v1 = bot_response['messages']
    print('LG0, messages_v1:', messages_v1)
    messages_v1_v1 = messages_v1[0]
    print('LG0, messages_v1_v1:', messages_v1_v1)
    messages_v1_v1_v1 = messages_v1_v1['content']
    print('LG0, messages_v1_v1_v1:', messages_v1_v1_v1)
    
    return messages_v1_v1_v1
                                
def format_response_text(response_txt):
    return {"messages": [{"type": "unstructured", "unstructured": {"text": str(response_txt)}}]}

def lambda_handler(event, context):
    # function needs to receive the input from the client, and send the messages to the lex bot for processing
    
    print('LG0, event:', event)
    print('LG0, context:', event)
    
    default_bot_response = 'There was an issue, please try again!'
    if event is None or 'messages' not in event.keys() or len(event['messages']) != 1:
         return {
            'statusCode': 200,
            'body': json.dumps(default_bot_response)
        }
    
    messages_received = event['messages']
    message_received = messages_received[0]
    text = message_received['unstructured']['text']
    
    print('LG0, messages_received:', messages_received)
    print('LG0, message_received:', message_received)
    print('LG0, text:', text)
    
    response_txt = send_to_lex_bot(str(text))  # without str(), we were throwing
    print('LG0, response_txt:', response_txt)
    output_response = format_response_text(response_txt)
    print('LG0, output_response:', output_response)
    
    return output_response
    
    # # TODO implement
    # return {
    #     'statusCode': 200,
    #     'body': json.dumps('Hello from Lambda!, LF0 from milad')
    # }
