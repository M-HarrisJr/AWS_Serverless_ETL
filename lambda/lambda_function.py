import boto3
import csv
from io import StringIO

s3 = boto3.client('s3')

def lambda_handler(event, context):
    # Get file from S3
    obj = s3.get_object(Bucket='raw-data-bucket-948451199216-us-east-2-an', Key='raw_sales.csv')
    data = obj['Body'].read().decode('utf-8').splitlines()
    
    reader = csv.DictReader(data)
    
    summary = {}

    # Transform: aggregate sales by region
    for row in reader:
        region = row['region']
        sales = float(row['sales'])
        quantity = int(row['quantity'])
        
        if region not in summary:
            summary[region] = {'sales': 0, 'quantity': 0}
        
        summary[region]['sales'] += sales
        summary[region]['quantity'] += quantity

    # Convert to CSV
    output = StringIO()
    writer = csv.writer(output)
    writer.writerow(['region', 'total_sales', 'total_quantity'])
    
    for region, values in summary.items():
        writer.writerow([region, values['sales'], values['quantity']])

    # Upload to processed bucket
    s3.put_object(
        Bucket='processed-data-bucket-948451199216-us-east-2-an',
        Key='summary.csv',
        Body=output.getvalue()
    )

    return {
        'statusCode': 200,
        'body': 'Pipeline run complete'
    }
