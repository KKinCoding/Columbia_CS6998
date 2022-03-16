import json
import boto3

# Define the client to interact with Lex
client = boto3.client('lex-runtime')

def lambda_handler(event, context):
    last_user_message = event['messages'][0]['unstructured']['text']
    # change this to the message that user submits on 
    # your website using the 'event' variable
    print(f"Message from frontend: {last_user_message}")
    response = client.post_text(botName='DiningConcierge',
                                botAlias='dine',
                                userId='testuser',
                                inputText=last_user_message)
    msg_from_lex = response['message']
    if msg_from_lex:
        print(f"Message from Chatbot: {msg_from_lex}")
        resp = {
            'statusCode': 200,
            "messages": [
                {
                    "type": "unstructured",
                    "unstructured": {
                        "text": msg_from_lex
                    }
                }    
            ]
        }
        # modify resp to send back the next question Lex would ask from the user
        
        # format resp in a way that is understood by the frontend
        # HINT: refer to function insertMessage() in chat.js that you uploaded
        # to the S3 bucket
        return resp