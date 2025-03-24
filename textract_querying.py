import os
import boto3
import json
from trp.t_pipeline import pipeline_merge_tables
import trp.trp2 as t2
from textractcaller.t_call import call_textract, Textract_Features

def upload_pdf_to_s3_uri(file_path, s3_uri):
    """
    Uploads a PDF file to an S3 location specified by URI
    
    Args:
        file_path (str): Local path to the PDF file
        s3_uri (str): Full S3 URI (e.g., 's3://bucket-name/folder/file.pdf')
    
    Returns:
        bool: True if upload successful, False otherwise
    """
    # Parse S3 URI
    s3_path = s3_uri.replace('s3://', '')
    bucket_name = s3_path.split('/')[0]
    s3_key = '/'.join(s3_path.split('/')[1:])
    
    # Initialize S3 client
    os.environ['AWS_Texttract_access_key'] = '<insert AWS access key>'
    os.environ['AWS_Texttract_secret_key'] = '<insert AWS secret key'
    s3_client = boto3.client('s3',
        aws_access_key_id=os.environ['AWS_Texttract_access_key'],
        aws_secret_access_key=os.environ['AWS_Texttract_secret_key'],
        region_name='us-west-2'
    )
    
    try:
        s3_client.upload_file(file_path, bucket_name, s3_key)
        print(f"Successfully uploaded {file_path} to {s3_uri}")
        return True
        
    except Exception as e:
        print(f"Error uploading file: {e}")
        return False

def get_textract_json(pdf_fp):

    s3_uri = f"s3://textract-console-us-west-2-f125f7b8-41af-4c7f-bc21-e81f2d2dc294/current_pdf.pdf"
    upload_pdf_to_s3_uri(pdf_fp, s3_uri)
        

    os.environ['AWS_Texttract_access_key'] = '<insert AWS access key>'
    os.environ['AWS_Texttract_secret_key'] = '<insert AWS secret key'
    session = boto3.Session(
        aws_access_key_id=os.environ['AWS_Texttract_access_key'],
        aws_secret_access_key=os.environ['AWS_Texttract_secret_key'],
        region_name='us-west-2'
    )
    textract_client = session.client('textract')
    textract_json = call_textract(
        input_document=s3_uri,
        features=[Textract_Features.FORMS, Textract_Features.TABLES, Textract_Features.LAYOUT],
        boto3_textract_client=textract_client
    )

    return textract_json