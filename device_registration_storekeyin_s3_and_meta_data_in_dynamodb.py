import json
import boto3
import logging

IOT_CORE_REGION='ap-southeast-1'
logger_aws_iot_core = logging.getLogger('example_logger')
logger_aws_iot_core.setLevel(logging.INFO)

# Define max item sizes for search pages
pageSize = 2


print('device registration is started')
# Obtain the IoT Client.
iot_client = boto3.client('iot')
dynamodb_client = boto3.client('dynamodb')
s3 = boto3.client('s3')

# def aws_iot_core_get_all_things(detail=False):
#     """
#     returns all the things registered in the aws-iot-core
#     """

#     # Return parameters
#     thingNames = []
#     thingArns = []

#     # Create client
#     # iot_client = boto3.client('iot', IOT_CORE_REGION)

#     # Parameters used to count things and search pages
#     things_count = 0
#     page_count = 0

#     # Log Info
#     if(detail):
#         logger_aws_iot_core.info("Getting things")

#     # Send the first request
#     response = iot_client.list_things(maxResults=pageSize)

#     # Count the number of the things until no more things are present on the search pages
#     while(1):
#         # Increment thing count
#         things_count = things_count + len(response['things'])
#         if(detail):
#             logger_aws_iot_core.info(
#                 f"\t{len(response['things'])} things are found on the {page_count+1}. page. Checking the next page ...")

#         # Append found things to the lists
#         for thing in response['things']:
#             thingArns.append(thing['thingArn'])
#             thingNames.append(thing['thingName'])

#         # Increment Page number
#         page_count += 1

#         # Check if nextToken is present for next search pages
#         if("nextToken" in response):
#             response = iot_client.list_things(
#                 maxResults=pageSize, nextToken=response["nextToken"])
#         else:
#             break

#     if(detail):
#         logger_aws_iot_core.info(
#             f"\tGetting things is completed. In total {things_count} things are found.")
#     return {"thingArns": thingArns, "thingNames": thingNames}



def aws_iot_core_get_all_thing_types(detail=False):
    """
    returns all the thing types registerd in the aws iot core
    """

    # Return parameters
    thingTypeNames = []
    thingTypeArns = []

    # Create client
    # iot_client = boto3.client('iot', IOT_CORE_REGION)

    # Parameter used to count thingType names
    types_count = 0
    page_count = 0

    # Log Info
    if(detail):
        logger_aws_iot_core.info("Getting thing types")

    # Send the first request
    response = iot_client.list_thing_types(maxResults=pageSize)

    # Count the number of the things until no more things are present on the search pages
    while(1):
        # Increment thing count
        types_count = types_count + len(response['thingTypes'])
        if(detail):
            logger_aws_iot_core.info(
                f"\t{types_count} thingTypes are found on the {page_count+1}. page. Checking the next page ...")

        # Append found thingTypes to the lists
        for thingType in response['thingTypes']:
            thingTypeArns.append(thingType['thingTypeArn'])
            thingTypeNames.append(thingType['thingTypeName'])

        # Increment Page number
        page_count += 1

        # Check if nextToken is present for next search pages
        if("nextToken" in response):
            response = iot_client.list_thing_types(
                maxResults=pageSize, nextToken=response["nextToken"])
        else:
            break

    if(detail):
        logger_aws_iot_core.info(
            f"\tGetting thingTypes is completed. In total {types_count} thingTypes are found.")
    return {"thingTypeArns": thingTypeArns, "thingTypeNames": thingTypeNames}



def lambda_handler(event, context):
    # print(event)
    if event['headers']['x-api-key'] == 'F77_iot_device_registration':
        # TODO implement
        # main code
        thing_type_name = 'ConnectedBulbs2'
        thing_type_description = 'My Connected Bulbs'
        
        
        data= event['body']
        
        print(data)
        data1=json.loads(data)
        
        
        
        device_name=data1['thingName']
        print(device_name)
        try:
            response = iot_client.describe_thing(
                    thingName=device_name
                    )
            print("after thing description search")
            response=str(device_name)+' thing already exisist'
            return {
                'statusCode': 200,
                'body': json.dumps(response)
                    }
        except:
            print("This is new resources so we need to create the this device")
        
            manufacturer=data1['attributes']['manufacturer']
            serial_number=data1['attributes']['serial_number']
            production_date=data1['attributes']['production_date']
            
            print(manufacturer)
            print(serial_number)
            print(production_date)
            
            
            policyName='testing'
            thingARN=''
            thing_group='testing_group'
            
            thingTypes =aws_iot_core_get_all_thing_types(detail=False)
            
            # print(thingTypes)
            if(thing_type_name in thingTypes["thingTypeNames"]):
                    print('thing type already exisist')
            else:
                iot_client.create_thing_type(
                    thingTypeName = thing_type_name,
                    thingTypeProperties = {
                    'thingTypeDescription': thing_type_description,
                    'searchableAttributes': [
                        'manufacturer',
                        'serial_number',
                        'production_date'
                        ]
                     }
                    )
                
            
            # thingNames = aws_iot_core_get_all_things(detail=False)
            
            
            # if(device_name in thingNames["thingNames"]):
            #         print(str(device_name)+' thing already exisist')
            #         response=str(device_name)+' thing already exisist'
            #         return {
            #         'statusCode': 200,
            #         'body': json.dumps(response)
            #             }
                    
                    
            # else:
              
            register_thing=iot_client.create_thing(
            thingName = device_name,
            thingTypeName = thing_type_name,
            attributePayload =  {
            'attributes': {
            'manufacturer': manufacturer,
            'serial_number': serial_number,
            'production_date': production_date
            }
            }
            )   
        
            # add to the thing group
            thingARN=register_thing['thingArn']
        
            response = iot_client.add_thing_to_thing_group(
            thingGroupName=thing_group,
            thingGroupArn='arn:aws:iot:ap-southeast-1:484265082862:thinggroup/testing_group',
            thingName=device_name,
            thingArn=thingARN,
            overrideDynamicGroups=True
            )
            
        
            # Create keys and certificates
            response = iot_client.create_keys_and_certificate(setAsActive=True)
            print(response)
            # Get the certificate and key contents
            certificateArn = response["certificateArn"]
            certificate = response["certificatePem"]
            key_public = response["keyPair"]["PublicKey"]
            key_private = response["keyPair"]["PrivateKey"]
        
            # log information
            logger_aws_iot_core.info(
                f"\t\tCreating the certificate...")
                
            
            bucket_name = "check-bick-iot-device-cetrs"
            directory_name = "my/key/"+device_name+"/"+device_name+"_private.pem.key" #it's name of your folders
            
            object = s3.put_object(Body=key_private,Bucket=bucket_name, Key=directory_name)
            
            directory_name = "my/key/"+device_name+"/"+device_name+"_publick.pem.key" #it's name of your folders
            
            object = s3.put_object(Body=key_public,Bucket=bucket_name, Key=directory_name)
            
            directory_name = "my/key/"+device_name+"/"+device_name+"_certificate.pem.crt" #it's name of your folders
            
            object = s3.put_object(Body=certificate,Bucket=bucket_name, Key=directory_name)
           
           
            
            # Attach certificate to things
            iot_client.attach_thing_principal(
                thingName=device_name, principal=certificateArn)
        
            logger_aws_iot_core.info(
                    f"\tAttaching thing {device_name} and certificate {certificateArn}...")
        
            # Attach policy to things
            logger_aws_iot_core.info(
                    f"\tAttaching thing {device_name} and policy {policyName}...")
            iot_client.attach_principal_policy(
                policyName=policyName, principal=certificateArn)
                
                
                
            # pushing meta data to the AWS Dynamodb
            
            data = dynamodb_client.put_item(
              TableName='bick_iot_device_group',
              Item={
                
                'device_name': {
                    'S': str(device_name)
                  },
                  'production_date': {
                    'S': str(production_date)
                  },
                  'serial_number': {
                    'S': str(serial_number)
                  },
                  'manufacturer': {
                    'S': str(manufacturer)
                  },
                  'thing_type_name': {
                    'S': str(thing_type_name)
                  },
                  'thing_group': {
                    'S': str(thing_group)
                  }
              })
            
            return {
                'statusCode': 200,
                'body': json.dumps(response)
            }
        
        
    
