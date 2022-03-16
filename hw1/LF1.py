import json
import boto3
from datetime import datetime

def get_response(slot, response, location, cuisine, people, date, time, email):
    return {
        "dialogAction": {
            "type": "ElicitSlot",
            "message": {
              "contentType": "PlainText",
              "content": response
            },
           "intentName": "DiningSuggestionsIntent",
           "slots": {
              "Location": location,
              "Cuisine": cuisine,
              "People": people,
              "Date": date,
              "Time": time,
              "Email": email
           },
           "slotToElicit" : slot
       }
    }

def lambda_handler(event, context):
    intentName = event["currentIntent"]["name"]
    
    if intentName == "GreetingIntent":
        return {
            "dialogAction": {
                "type": "Close",
                "fulfillmentState": "Fulfilled",
                "message": {
                    "contentType": "PlainText",
                    "content": "Hi there, how can I help you?"
                }
            }
        }
        
    elif intentName == "ThankYouIntent":
        return {
            "dialogAction": {
                "type": "Close",
                "fulfillmentState": "Fulfilled",
                "message": {
                    "contentType": "PlainText",
                    "content": "You are welcome."
                }
            }
        } 
        
    elif intentName == "DiningSuggestionsIntent":
        slots = event["currentIntent"]["slots"]
        print(slots)
        location, cuisine, people, date, time, email = slots["Location"], slots["Cuisine"], slots["People"], slots["Date"], slots["Time"], slots["Email"]
        if not location:
            response = "Great. I can help you with that. What city or city area are you looking to dine in?"
            return get_response("Location", response, location, cuisine, people, date, time, email)
        elif not cuisine:
            response = "Got it, " + location + ". What cuisine would you like to try?"
            return get_response("Cuisine", response, location, cuisine, people, date, time, email)
        elif not people:
            response = "Ok, " + cuisine + " sounds good! How many people are in your party?"
            return get_response("People", response, location, cuisine, people, date, time, email)
        elif not date:
            if int(people) <= 0:
                response = "Sorry, the number looks invalid. How many people are in your party?"
                return get_response("People", response, location, cuisine, people, date, time, email)
            response = "Got it, " + people + ". A few more to go. What date?"
            return get_response("Date", response, location, cuisine, people, date, time, email)
        elif not time:
            today = datetime.today().strftime('%Y-%m-%d')
            if date < today:
                response = "Sorry, the date looks invalid. What date?"
                return get_response("Date", response, location, cuisine, people, date, time, email)
            response = date + " and what time?"
            return get_response("Time", response, location, cuisine, people, date, time, email)
        elif not email:
            response = time + " is great! Lastly, I need your email so I can send you my findings."
            return get_response("Email", response, location, cuisine, people, date, time, email)
        
        # now all slots filled, push to SQS
        message = ",".join([location, cuisine, people, date, time, email])
        sqs = boto3.client('sqs')
        queue_url = "https://sqs.us-east-1.amazonaws.com/642527119266/OrderQueue"
        response = sqs.send_message(
            QueueUrl = queue_url,
            MessageBody = message
        )

        return {
            "dialogAction": {
                "type": "Close",
                "fulfillmentState": "Fulfilled",
                "message": {
                    "contentType": "PlainText",
                    "content": "You’re all set. Expect my suggestions shortly! Have a good day."
                }
            }
        }
    
    else:
        return {
            "dialogAction": {
                "type": "Close",
                "fulfillmentState": "Fulfilled",
                "message": {
                    "contentType": "PlainText",
                    "content": "Sorry, please specify your needs."
                }
            }
        }