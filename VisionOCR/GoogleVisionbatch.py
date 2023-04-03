# -*- coding: utf-8 -*-
"""
Created on Tue Feb  8 10:54:14 2022

@author: sct8690
"""
import json
import re
import os
from google.cloud import vision
from google.cloud import storage
    
os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = "key.json"

#Use Google Vision to OCR a pdf file stored in google cloud and write the results to a local text file
def async_detect_document(gcs_source_uri, gcs_destination_uri, ocr_destination_path):
    """OCR with PDF/TIFF as source files on GCS"""

    # Supported mime_types are: 'application/pdf' and 'image/tiff'
    mime_type = 'application/pdf'

    # How many pages should be grouped into each json output file.
    batch_size = 100

    client = vision.ImageAnnotatorClient()

    feature = vision.Feature(
        type_=vision.Feature.Type.DOCUMENT_TEXT_DETECTION)

    gcs_source = vision.GcsSource(uri=gcs_source_uri)
    input_config = vision.InputConfig(
        gcs_source=gcs_source, mime_type=mime_type)

    gcs_destination = vision.GcsDestination(uri=gcs_destination_uri)
    output_config = vision.OutputConfig(
        gcs_destination=gcs_destination, batch_size=batch_size)

    async_request = vision.AsyncAnnotateFileRequest(
        features=[feature], input_config=input_config,
        output_config=output_config)

    operation = client.async_batch_annotate_files(
        requests=[async_request])

    print('Waiting for the operation to finish.')
    operation.result(timeout=420)   

    # Once the request has completed and the output has been
    # written to GCS, we can list all the output files.
    storage_client = storage.Client()

    match = re.match(r'gs://([^/]+)/(.+)', gcs_destination_uri)
    bucket_name = match.group(1)
    prefix = match.group(2)

    bucket = storage_client.get_bucket(bucket_name)

    # List objects with the given prefix.
    blob_list = list(bucket.list_blobs(prefix=prefix)) 
    print('Output files:')
    for blob in blob_list:
        print(blob.name)

    # Process the output files from GCS.
    # Since we specified batch_size=100, the first response contains
    # the first 100 pages of the input file.
    
    output = blob_list[0]

    json_string = output.download_as_string()
    response = json.loads(json_string)

    #make a local file to write the contents of this batch into
    os.makedirs(os.path.dirname("OCRresults/" + ocr_destination_path + ".txt"), exist_ok=True)
    file = open("OCRresults/" + ocr_destination_path + ".txt", "a")
    
    # Print the OCRed text page by page into the local file
    
    pages_response = response['responses']
    for page in pages_response:
        annotation = page['fullTextAnnotation']
        file.write(annotation['text'])
        file.write('##################################\n')
        


#iterate through all the files in a directory and run the OCR function on them       
def run_a_batch (source_bucket, destination_bucket, source_dir):


    storage_client = storage.Client()
    bucket=storage_client.get_bucket(source_bucket)
    blob_list = list(bucket.list_blobs(prefix=source_dir)) 
    print('Input files:')
    for blob in blob_list:
        if blob.name != source_dir + '/':
            size = len(blob.name)
            filepath = blob.name[:size - 4]
            dir_size = len(source_dir) + 1
            filename = filepath[dir_size:]
            async_detect_document('gs://' + source_bucket + '/' + blob.name, 'gs://' + destination_bucket + '/' + filename + '_', filepath)
        
        
#####This is where you call the function. To process a batch of files, specify three parameters here: 
#(1) The Google cloud bucket where you're putting the source files, (2) The Google cloud bucket where the JSON results should go, (3) the name of 
# the folder your source files are in within the source bucket. The first two parameters will be the same every time. The third parameter gives you
#the option of keeping your documents organized into different folders, although if you don't want to do that you can just put them all into 
#the same folder.
run_a_batch('[SOURCE_DIR]', '[RESULT_DIR]', 'docs')
  
        


