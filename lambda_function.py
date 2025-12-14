from s3_util import read_all_file_names_from_s3, read_multiple_csv_files_from_s3, save_s3_csv_file


def lambda_handler(event, context):
    source_bucket = "click-s3-glue-redshift"  # Replace with your source bucket name
    source_folder_prefix = 'temp/'  # Optional: replace with your source folder/prefix
    file_keys = read_all_file_names_from_s3(source_bucket, source_folder_prefix)
    # read all file and combine into single one
    combined_data = read_multiple_csv_files_from_s3(source_bucket, file_keys)
    # save in s3
    destination_bucket = "click-s3-glue-redshift"
    destination_key = "bronze/combined_data.csv"
    save_s3_csv_file(destination_bucket, destination_key, combined_data)
    print(f"Successfully combined {len(combined_data) - 1} data rows into {destination_key} in {destination_bucket}")

    return {
        'statusCode': 200,
        'body': f'Combined CSV files saved to s3://{destination_bucket}/{destination_key}'
    }
