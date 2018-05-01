# -*- coding: utf-8 -*-
from __future__ import unicode_literals

import os

import boto3
import botocore
import time
from django.http import JsonResponse
from django.shortcuts import render
import sendgrid
from sendgrid.helpers.mail import *

# Create your views here.
from django.views.decorators.csrf import csrf_exempt


@csrf_exempt
def convertVoices(request):
    BUCKET_NAME = os.environ.get('BUCKET_NAME')
    QUEUE_URL = os.environ.get('QUEUE_URL')
    QUEUE_NAME = os.environ.get('QUEUE_NAME')
    SENDGRID_APIKEY = os.environ.get('SENDGRID_APIKEY')

    s3 = boto3.resource('s3', aws_access_key_id=os.environ.get('aws_access_key_id'),
                        aws_secret_access_key=os.environ.get('aws_access_secret'), )
    sqs = boto3.resource('sqs', aws_access_key_id=os.environ.get('aws_access_key_id'),
                         aws_secret_access_key=os.environ.get('aws_access_secret'), region_name='us-east-1')
    dynamodb = boto3.client('dynamodb', aws_access_key_id=os.environ.get('aws_access_key_id'),
                            aws_secret_access_key=os.environ.get('aws_access_secret'), region_name='us-east-1')

    # SQS LOGIC
    queue = sqs.get_queue_by_name(QueueName=QUEUE_NAME)

    message_body = ''
    user_id = ''
    file_name = ''
    message_object = None

    while True:

        for message in queue.receive_messages(MaxNumberOfMessages=1, MessageAttributeNames=['All']):
            message_body = message.body
            user_id = message.message_attributes.get('id').get('StringValue')
            file_name = message.message_attributes.get('nombre').get('StringValue')
            message_object = message

        # VALIDATING THERE IS AN INCOMMING MSG FROM SQS
        if user_id != '':
            print ('PROCESSING NEW MESSAGE --- ')
            # S3 LOGIC
            try:
                s3.Bucket(BUCKET_NAME).download_file(file_name, file_name)
            except botocore.exceptions.ClientError as e:
                if e.response['Error']['Code'] == "404":
                    print("The object does not exist.")
                else:
                    raise
            # FFMPEG LOGIC
            voice_status = 'Exitosamente'
            voice_msg = '<h1>Hola!</h1>' + '<p>Tu voz has sido procesada y publicada en la página del concurso.</p>' + '<p>Gracias por participar!</p>'
            try:
                convertion_file = file_name
                base = os.path.splitext(convertion_file)[0]
                process_file_name = 'process_' + base + '.mp3'
                os.rename(convertion_file, process_file_name)
                # S3 UPLOAD CONVERTED FILE
                data = open(process_file_name, 'rb')
                s3.Bucket(BUCKET_NAME).put_object(Key=process_file_name, Body=data)
            except:
                voice_status = 'Fallidamente'
                voice_msg = '<h1>Hola!</h1>' + '<p>Tu voz no se ha podido procesar con exito.</p>' + '<p> Estaremos en contacto contigo para brindarte asesoria en el proceso. Gracias</p>'
                print ("FFMPEG Error. Line - 34")
            # SENDGRID LOGIC
            # DYNAMODB LOGIC TO GET USER EMAIL
            try:

                voice_item = dynamodb.get_item(
                    TableName="voice",
                    Key={"id": {"S": str(user_id)}}
                )
                user_email = voice_item['Item']['email']['S']
                print (user_email)
            except:
                print('DynamoDB - Fecthing User Email Error')
            sg = sendgrid.SendGridAPIClient(apikey=SENDGRID_APIKEY)
            data = {
                "personalizations": [
                    {
                        "to": [
                            {
                                "email": user_email
                            }
                        ],
                        "subject": "Tu voz se ha procesado " + voice_status + "."
                    }
                ],
                "from": {
                    "email": "supervoicescolombia@contest.com"
                },
                "content": [
                    {
                        "type": "text/html",
                        "value": voice_msg
                    }
                ]
            }
            response = sg.client.mail.send.post(request_body=data)
            os.remove(process_file_name)
            message_object.delete()
            user_id = ''
            time.sleep(3)
        else:
            print ('NOT INCOMMING MESSAGES!')
    context = {
        'Mensaje': 'Finalizó la carga del log'
    }
    return JsonResponse(context)