'''This program is designed to demonstrate the various functions associated
with AWS s3 Buckets.

The program will demonstrate deleting and creating buckets as well as
deleting, creating, uploading, copying, and downloading files within them'''
import sys
import datetime
import logging
from random import randint
import boto3
import botocore
from botocore.exceptions import ClientError

logging.basicConfig(filename='./error.log',
                    format='%(asctime)s %(levelname)s:%(message)s',
                    datefmt='%Y-%m-%d %H:%M:%S',
                    filemode='w',
                    level=logging.INFO)

def create(name,region=None):
    '''This function is dedicated to creating a bucket based on user input'''
    if region is None:
        s3_client = boto3.client('s3')
        s3_client.create_bucket(Bucket=name)

def upload(name,bucket,object_name=None):
    '''This function is dedicated to uploading files into the bucket the
    user chooses'''
    if object_name is None:
        object_name = name

    s3_client = boto3.client('s3')
    try:
        s3_client.upload_file(name, bucket, object_name)

    except ClientError as e:
        logging.error(e)
        return False
    return True

def delete_obj(bucket, object_name):
    '''This function is dedicated to selecting a file within a bucket and
    deleting said file'''
    s3_client = boto3.client('s3')

    try:
        s3_client.delete_object(Bucket=bucket,Key=object_name)

    except ClientError as e:
        logging.error(e)
        return False
    return True

def delete(bucket_name):
    '''This function is dedicated to deleting an empty bucket'''
    s3_client = boto3.client('s3')
    try:
        s3_client.delete_bucket(Bucket=bucket_name)

    except ClientError as e:
        logging.error(e)
        return False
    return True

def copy(source_bucket, source_object,
                dest_bucket, dest_object_name=None):
    '''This function is dedicated to copying a file from a source bucket
    into a destination bucket. Source and destination are chosen by user'''
    copy_source = {'Bucket': source_bucket, 'Key': source_object}
    if dest_object_name is None:
        dest_object_name = source_object

    s3_client = boto3.client('s3')
    try:
        s3_client.copy_object(CopySource=copy_source, Bucket=dest_bucket,
                       Key=dest_object_name)
    except ClientError as e:
        logging.error(e)
        return False
    return True

def bucket_exists(bucket_name):
    '''This function checks to see if a bucket already exists within the AWS s3
    service'''
    s3_client = boto3.client('s3')
    try:
        s3_client.head_bucket(Bucket=bucket_name)
    except ClientError as e:
        logging.debug(e)
        return False
    return True

def list_buckets():
    '''This function is dedicated to populating and listing out all created
    buckets in a numbered format'''
    s3_client = boto3.client('s3')
    response = s3_client.list_buckets()
    buckets = [bucket['Name'] for bucket in response['Buckets']]

    var_x=0
    while var_x < len(buckets):
        for bucket in buckets:
            print(f"{var_x}: {bucket}")
            var_x = var_x+1

def list_obj(bucket_name):
    '''This function is dedicated to parsing out all "Key" names of objects within
    a bucket in preparation to be listed'''
    s3_client = boto3.client('s3')
    try:
        response = s3_client.list_objects_v2(Bucket=bucket_name)
    except ClientError as e:
        logging.error(e)
        return None
    return response['Contents']

def menu_select():
    '''This function prints the menu for the user to use'''
    print("1: Create a new bucket")
    print("2: Add file to new bucket")
    print("3: Delete file in bucket")
    print("4: Delete Bucket")
    print("5: Copy object to another bucket")
    print("6: Download object from bucket")
    print("7: Exit Program")

def main():
    '''This is where the main code resides based on the selection the user
    made in the menu_select function'''
    selection = None
    current_filename = None
    #construct while loop over all choices so alwats returns to root menu
    while selection != '7':
        menu_select()
        selection = input("Make a selection: ")

        if selection == '1':

            logging.basicConfig(level=logging.DEBUG,
                        format='%(levelname)s: %(asctime)s: %(message)s')
            #prompts user for first name
            while True:
                first = input("What is your first name?")
                lower_first = first.lower()

                if lower_first.isalpha():
                    break

                print("Please enter a valid first name")
                logging.error('Incorrect Input: "%s"',first)
            #prompts user for last name
            while True:
                last = input("What is your last name?")
                lower_last = last.lower()

                if lower_last.isalpha():
                    break

                print("Please enter a valid last name")
                logging.error('Incorrect Input: "%s"',last)
            #concatenates first and last name and calls random for 6 digits
            current_filename = lower_first+lower_last+\
                str(randint(100000,999999))
            #if-else checks if bucket of same name exists then logs it
            if not bucket_exists(current_filename):
                logging.info('%s has been created',current_filename)
                print(f'{current_filename} has been created')
                create(current_filename,None)
            else:
                logging.info('''%s already exists; choose \
                    another name.''',current_filename)

        elif selection == '2':
            selected_bucket = None
            list_buckets()#function to call a numbered list of buckets
            s3_client = boto3.client('s3')
            response = s3_client.list_buckets()
            buckets = [bucket['Name'] for bucket in response['Buckets']]

            logging.basicConfig(level=logging.DEBUG,
                format='%(levelname)s: %(asctime)s: %(message)s')
            #prompt user to choose a bucket for upload
            flag = True

            while flag is True:
                try:
                    selection_2 = int(input("Select a bucket to upload the file: "))
                except ValueError:
                    print("Please select a valid number")
                    logging.error('Incorrect Input: "%d"',selection_2)
                else:
                    if 0<= selection_2 < len(buckets):
                        selected_bucket = buckets[selection_2]
                        break
                    print("Please select a valid number")
                    logging.error('Incorrect Input: "%d"',selection_2)
            #uploads file to selected bucket as new file: 'new_payload.txt'
            response = upload('payload.txt', selected_bucket, 'new_payload.txt')
            if response:
                logging.info('File Successfully Uploaded to %s',selected_bucket)
                print(f'File Successfully Uploaded to {selected_bucket}')
            else:
                print("File did not upload")

        elif selection == '3':
            selected_bucket = None
            selected_file = None
            list_buckets()
            s3_client = boto3.client('s3')
            response = s3_client.list_buckets()
            buckets = [bucket['Name'] for bucket in response['Buckets']]

            logging.basicConfig(level=logging.DEBUG,
                format='%(levelname)s: %(asctime)s: %(message)s')
            #prompt user for bucket deletion
            flag = True
            while flag is True:
                try:
                    selection_3 = int(input("Select a file to delete: "))
                except ValueError:
                    print("Please select a valid number")
                    logging.error('Incorrect Input: "%d"',selection_3)
                else:
                    if 0 <= selection_3 < len(buckets):
                        selected_bucket = buckets[selection_3]
                        break
                    print("Please select a valid number")
                    logging.error('Incorrect Input: "%d"',selection_3)
            #call list_obj function to list out files within a bucket
            objects = list_obj(selected_bucket)
            if objects is not None:
                print(f'Objects in {selected_bucket}')
            var_x = 0
            while var_x < len(objects):#lists out the items with for-loop
                for obj in objects:
                    print(f'{var_x}: {obj["Key"]}')
                    var_x = var_x + 1
            #prompt user to select a file to delete
            while flag is True:
                try:
                    selection_3a = int(input("Select a file to delete: "))
                except ValueError:
                    print("Please select a valid number")
                    logging.error('Incorrect Input: "%d"',selection_3a)
                else:
                    if 0 <= selection_3a < len(objects):
                        break
                    print("Please select a valid number")
                    logging.error('Incorrect Input: "%d"',selection_3a)
            #used to assign number equivalent value (y) to dictionary item
            var_y = 0
            for obj in objects:
                if var_y == selection_3a:
                    selected_file = obj["Key"]
                    break
                var_y = var_y + 1
            #delete file and log action in error log file
            if delete_obj(selected_bucket,selected_file):
                print(f'{selected_file} deleted from {selected_bucket}')
                logging.info('%s deleted from %s',selected_file,selected_bucket)

        elif selection == '4':
            list_buckets()
            s3_client = boto3.client('s3')
            response = s3_client.list_buckets()
            buckets = [bucket['Name'] for bucket in response['Buckets']]

            logging.basicConfig(level=logging.DEBUG,
                format='%(levelname)s: %(asctime)s: %(message)s')

            while True:
                try:
                    bucket_delete= int(input("What bucket should be deleted: "))
                except ValueError:
                    print("Please enter a valid number")
                    logging.error('Incorrect Input: "%d"',bucket_delete)
                else:
                    if 0 < bucket_delete < len(buckets):
                        break
                    print("Please enter a valid number")
                    logging.error('Incorrect Input: "%d"',bucket_delete)
            #deletes bucket if empty, if not then user is prompted and logged
            if delete(buckets[bucket_delete]):
                print(f'{buckets[bucket_delete]} was deleted')
                logging.info('%s was deleted',buckets[bucket_delete])
            else:
                print(f'{buckets[bucket_delete]} is not empty')
                logging.info('%s is not empty',buckets[bucket_delete])

        elif selection == '5':

            source_bucket = None
            source_file = None
            dest_bucket = None
            list_buckets()
            s3 = boto3.client('s3')
            response = s3.list_buckets()
            buckets = [bucket['Name'] for bucket in response['Buckets']]

            logging.basicConfig(level=logging.DEBUG,
                format='%(levelname)s: %(asctime)s: %(message)s')
            #promts user to select bucket and file then bucket to copy to
            flag = True

            while flag is True:
                try:
                    selection_5 = int(input("Select a bucket to copy from: "))
                except ValueError:
                    print("Please enter a valid number")
                    logging.error('Incorrect Input: "%d"',selection_5)
                else:
                    if 0 <= selection_5 < len(buckets):
                        source_bucket = buckets[selection_5]
                        break
                    print("Please enter a valid number")
                    logging.error('Incorrect Input: "%d"',selection_5)
            #list buckets then objects for user to select file to copy
            objects = list_obj(source_bucket)
            if objects is not None:
                print(f'Objects in {source_bucket}')
            var_x = 0
            while var_x < len(objects):
                for obj in objects:
                    print(f'{var_x}: {obj["Key"]}')
                    var_x = var_x + 1
            while flag is True:
                try:
                    selection_5a = int(input("Select a file to copy: "))
                except ValueError:
                    print("Please enter a valid number")
                    logging.error('Incorrect Input: "%d"',selection_5)
                else:
                    if 0 <= selection_5a < len(objects):
                        break
                    print("Please select a valid number")
                    logging.info('Incorrect Input: "%d"',selection_5a)
            #assign dictionary value a number like in 3rd selection
            var_y = 0
            for obj in objects:
                if var_y == selection_5a:
                    source_file = obj["Key"]
                    break

                var_y = var_y + 1

            print(source_file)

            list_buckets()#list buckets 2nd time for destination to be chosen

            while flag is True:
                try:
                    selection_5b = int(input("Select destination bucket: "))
                except ValueError:
                    print("Please select a valid number")
                    logging.error('Incorrect Input: "%d"',selection_5b)
                else:
                    if 0 <= selection_5b < len(buckets):
                        dest_bucket = buckets[selection_5b]
                    else:
                        print("Please select a valid number")
                        logging.error('Incorrect Input: "%d"',selection_5b)
                #if same bucket is chosen then an error occurs
                if source_bucket == dest_bucket:
                    print('Cannot choose same bucket')
                    logging.error("Duplicate bucket chosen for copying")
                else:
                    break
            #indicates success of copying file from bucket to bucket
            success = copy(source_bucket,source_file,dest_bucket)

            if success:
                print(f'Copied {source_bucket}/{source_file} to '
                     f'{dest_bucket}')
                logging.info('Copied %s/%s to '
                     '%s',source_bucket,source_file,dest_bucket)

        elif selection == '6':
            selected_bucket = None
            selected_file = None
            list_buckets()
            s3_client = boto3.client('s3')
            response = s3_client.list_buckets()
            buckets = [bucket['Name'] for bucket in response['Buckets']]

            logging.basicConfig(level=logging.DEBUG,
                format='%(levelname)s: %(asctime)s: %(message)s')
            #prompt user to select bucket then file to download to local PC
            flag = True
            while flag is True:
                try:
                    selection_6 = int(input("Select bucket to download from: "))
                except ValueError:
                    print("Please select a valid number")
                    logging.error('Incorrect Input: "%d"',selection_6)
                else:
                    if 0 <= selection_6 < len(buckets):
                        selected_bucket = buckets[selection_6]
                        break
                    print("Please select a valid number")
                    logging.error('Incorrect Input: "%d"',selection_6)

            objects = list_obj(selected_bucket)
            if objects is not None:
                print(f'Objects in {selected_bucket}')
            var_x = 0
            while var_x < len(objects):
                for obj in objects:
                    print(f'{var_x}: {obj["Key"]}')
                    var_x = var_x + 1
            while flag is True:
                try:
                    selection_6a = int(input("Select a file to download: "))
                except ValueError:
                    print("Please select a valid number")
                    logging.info('Incorrect Input: "%d"',selection_6a)
                else:
                    if 0 <= selection_6a < len(objects):
                        break
                    print("Please select a valid number")
                    logging.info('Incorrect Input: "%d"',selection_6a)
            var_y = 0
            for obj in objects:
                if var_y == selection_6a:
                    selected_file = obj["Key"]
                    break

                var_y = var_y + 1
            #create resouce object for s3 bins
            s3_resource = boto3.resource('s3')
            #attempt to download selected file to local PC, 404 error logged if failure
            try:
                s3_resource.Bucket(selected_bucket).download_file(selected_file,
                'downloaded_payload.txt')#download as 'downloaded_payload.txt'
                print(f'{selected_file} has been downloaded locally from\n'
                    f'{selected_bucket} as "downloaded_payload.txt"')
                logging.info('%s has been downloaded locally from %s\n'
                    ' as "downloaded_payload.txt"',selected_file,selected_bucket)
            except botocore.exceptions.ClientError as e:
                if e.response['Error']['Code'] == "404":
                    print("The file does not exist.")
                else:
                    raise

        elif selection == '7':
            d_t = datetime.datetime.now()
            print("Thank you for using the system")
            print("Exit date:",d_t.strftime("%m/%d/%Y"))
            print("Exit time:",d_t.strftime("%I:%M:%S %P"))
            sys.exit()#exit program when user selects

        else:
            print('Please make a valid selection')

main()#run main program
