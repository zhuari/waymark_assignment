'''
Aric Zhu
'''

import boto3
import pandas as pd
from datetime import datetime


# Step 1: Data Transformation

def s3_api_call(object_key, bucket_name='waymark-assignment', 
                aws_access_key_id='AKIAZLXG4RYJBLE4OTXT', 
                aws_secret_access_key='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'):
    '''Read in a csv file from a public S3 bucket, 
    then remove columns and rows that are all NaN 
    '''
    csv_file = pd.read_csv(
        f's3://{bucket_name}/{object_key}',
        storage_options = {
            'key': aws_access_key_id,
            'secret': aws_secret_access_key,
        },
    )
    # removes rows and columns that are all NaN
    csv_file = csv_file.dropna(axis=0, how='all')
    csv_file = csv_file.dropna(axis=1, how='all')
    return csv_file


def convert_to_dt(series, format='%m/%d/%y'):
    '''Convert and format a date stored as a string type into a datetime type
    '''
    date = pd.to_datetime(series, format=format)
    return date
    

pt_enroll = s3_api_call(object_key='patient_id_month_year.csv')
# format enrollment month into standard date format YYYY-MM-DD
pt_enroll['month_year'] = convert_to_dt(pt_enroll['month_year'])
pt_enroll = pt_enroll.sort_values(by=['patient_id', 'month_year'])


def find_consecutive_months(enroll_month):
    '''A patient can have multiple continuous enrollment periods if their enrollment
    is interrupted for a month, so they can have multiple enrollment_start_date and enrollment_end_date values; 
    this function labels a patient's distinct enrollment period(s) with a unique ID
    '''
    curr_enroll_month = enroll_month - pd.DateOffset(months=1)
    prev_enroll_month = enroll_month.shift(1)
    consecutive_month = curr_enroll_month != prev_enroll_month
    return consecutive_month.cumsum()


pt_enroll['subgroup'] = pt_enroll.groupby('patient_id').transform(find_consecutive_months)['month_year']

# create final patient_enrollment_span table of each patient's distinct enrollment period start and end dates
patient_enrollment_span = pt_enroll.groupby(['patient_id', 'subgroup']).agg(
    patient_id = ('patient_id', 'first'),
    enrollment_start_date = ('month_year', 'min'),
    enrollment_end_date = ('month_year', 'max')
).reset_index(drop=True)

patient_enrollment_span.to_csv('patient_enrollment_span.csv', index=False)
print(f'Number of rows in patient_enrollment_span.csv: {len(patient_enrollment_span)}')


# Step 2: Data Aggregation

opt_visits = s3_api_call(object_key='outpatient_visits_file.csv')
# format outpatient visit date into standard date format YYYY-MM-DD
opt_visits['date'] = convert_to_dt(opt_visits['date'])
# sum outpatient_visit_count for duplicate dates to remove duplicate dates
opt_visits = opt_visits.groupby(['patient_id', 'date']).sum().reset_index()
# rollback visit date to the first date of the month
opt_visits['opt_visit_month'] = opt_visits['date'] - pd.offsets.MonthBegin()

# create a table of each patient's count of outpatient visits and
# days with outpatient visits by month
opt_visits_grouped = opt_visits.groupby(['patient_id', 'opt_visit_month']).agg(
    patient_id = ('patient_id', 'first'),
    opt_visit_month = ('opt_visit_month', 'min'),
    ct_outpatient_visits = ('outpatient_visit_count', 'sum'),
    ct_days_with_outpatient_visit = ('date', 'nunique')
).reset_index(drop=True)

# instead of merging outpatient visits with the final output patient_enrollment_span, 
# it's easier to merge with the penultimate pre-groupby table (pt_enroll) and do a groupby after
pt_enroll_visits = pt_enroll.merge(
    opt_visits_grouped, how='left', 
    left_on=['patient_id', 'month_year'], 
    right_on=['patient_id', 'opt_visit_month']
)

# create final results table of outpatient visits by continuous enrollment period
result = pt_enroll_visits.groupby(['patient_id', 'subgroup']).agg(
    patient_id = ('patient_id', 'first'),
    enrollment_start_date = ('month_year', 'min'),
    enrollment_end_date = ('month_year', 'max'),
    ct_outpatient_visits = ('ct_outpatient_visits', 'sum'),
    ct_days_with_outpatient_visit = ('ct_days_with_outpatient_visit', 'sum')
).reset_index(drop=True)

result.to_csv('result.csv', index=False)
print(f"Distinct values of ct_days_with_outpatient_visit in result.csv: {result['ct_days_with_outpatient_visit'].nunique()}")
