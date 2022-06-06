from pybaseball import statcast
from datetime import date
from datetime import timedelta
from io import StringIO
import pandas as pd
import os
import boto3

session = boto3.Session(
aws_access_key_id=  ${{ secrets.AWS_ACCESS_KEY_ID }},
aws_secret_access_key= ${{ secrets.AWS_SECRET_ACCESS_KEY }}
)

today = date.today()
yesterday_date = today - timedelta(days = 1)
yesterday = str(yesterday_date)

print(f"statcast_data{yesterday}")

statcast_data = statcast(start_dt = yesterday, end_dt = yesterday)

s3_res = session.resource('s3')

csv_buffer = StringIO()
statcast_data.to_csv(csv_buffer)

bucket_name = 'statcast-data-2022'
s3_object_name = f"statcast_data_{yesterday}"
s3_res.Object(bucket_name, s3_object_name).put(Body=csv_buffer.getvalue())


