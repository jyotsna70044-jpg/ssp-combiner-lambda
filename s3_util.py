import csv
import io
import boto3


def read_all_file_names_from_s3(bucket_name, folder_prefix):
    all_keys = []
    s3_client = boto3.client('s3')

    paginator = s3_client.get_paginator('list_objects_v2')
    pages = paginator.paginate(Bucket=bucket_name, Prefix=folder_prefix)
    for page in pages:
        if 'Contents' in page:
            for obj in page['Contents']:
                key = obj['Key']
                # Exclude objects that are just folder markers (end with /)
                if not key.endswith('/'):
                    all_keys.append(key)
    return all_keys


def read_multiple_csv_files_from_s3(bucket_name, file_keys):
    s3_client = boto3.client('s3')
    # Use an in-memory buffer to build the combined CSV
    combined_csv_buffer = io.StringIO()
    writer = csv.writer(combined_csv_buffer)
    # Flag to track if the header has been written
    header_written = False

    for key in file_keys:
        response = s3_client.get_object(Bucket=bucket_name, Key=key)
        lines = response['Body'].read().decode('utf-8').splitlines()
        # Read the CSV content
        reader = csv.reader(lines)
        # Write the header only once
        if not header_written:
            header = next(reader)
            writer.writerow(header)
            header_written = True
        else:
            # Skip the header row for subsequent files
            next(reader)
        # Write the remaining rows
        writer.writerows(reader)

    # Get the combined CSV data as a string
    combined_data = combined_csv_buffer.getvalue()
    return combined_data


def read_single_csv_files_from_s3(bucket_name, file_key):
    s3 = boto3.client('s3')
    try:
        response = s3.get_object(Bucket=bucket_name, Key=file_key)
        csv_content = response['Body'].read().decode('utf-8')
        # Use io.StringIO to treat the string content as a file
        data_file = io.StringIO(csv_content)
        reader = csv.reader(data_file)
        # Read headers
        headers = next(reader)
        # Read data rows
        data = [row for row in reader]
        return headers, data
    except Exception as e:
        print(f"Error reading CSV from S3: {e}")
        return None, None


def save_s3_csv_file(bucket, key, csv_string):
    s3 = boto3.client('s3')
    try:
        s3.put_object(Bucket=bucket, Key=key, Body=csv_string)
        print(f"CSV file uploaded to s3://{bucket}/{key}")
    except Exception as e:
        print(f"Error uploading to S3: {e}")
