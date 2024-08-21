import boto3
import pandas as pd
from datetime import datetime


# Step 1

def s3_api_call(object_key, bucket_name='waymark-assignment', 
                aws_access_key_id='AKIAZLXG4RYJBLE4OTXT', 
                aws_secret_access_key='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'):
    csv_file = pd.read_csv(
        f's3://{bucket_name}/{object_key}',
        storage_options = {
            'key': aws_access_key_id,
            'secret': aws_secret_access_key,
        },
    )
    
    csv_file = csv_file.dropna(axis=0, how='all')
    csv_file = csv_file.dropna(axis=1, how='all')
    return csv_file


def convert_to_dt(series, format='%m/%d/%y'):
    date = pd.to_datetime(series, format=format)
    return date
    

pt_enroll = s3_api_call(object_key='patient_id_month_year.csv')
pt_enroll['month_year'] = convert_to_dt(pt_enroll['month_year'])
pt_enroll = pt_enroll.sort_values(by=['patient_id', 'month_year'])


def find_consecutive_months(enroll_month):
    curr_enroll_month = enroll_month - pd.DateOffset(months=1)
    prev_enroll_month = enroll_month.shift(1)
    consecutive_month = curr_enroll_month != prev_enroll_month
    return consecutive_month.cumsum()


pt_enroll['subgroup'] = pt_enroll.groupby('patient_id').transform(find_consecutive_months)['month_year']

patient_enrollment_span = pt_enroll.groupby(['patient_id', 'subgroup']).agg(
    patient_id = ('patient_id', 'first'),
    enrollment_start_date = ('month_year', 'min'),
    enrollment_end_date = ('month_year', 'max')
).reset_index(drop=True)

patient_enrollment_span.to_csv('patient_enrollment_span.csv', index=False)
print(len(patient_enrollment_span))


# Step 2
opt_visits = s3_api_call(object_key='outpatient_visits_file.csv')
opt_visits['date'] = convert_to_dt(opt_visits['date'])

# combine outpatient_visit_count of repeating dates
opt_visits = opt_visits.groupby(['patient_id', 'date']).sum().reset_index()
opt_visits['opt_visit_month'] = opt_visits['date'] - pd.offsets.MonthBegin()

opt_visits_grouped = opt_visits.groupby(['patient_id', 'opt_visit_month']).agg(
    patient_id = ('patient_id', 'first'),
    opt_visit_month = ('opt_visit_month', 'min'),
    ct_outpatient_visits = ('outpatient_visit_count', 'sum'),
    ct_days_with_outpatient_visit = ('date', 'nunique')
).reset_index(drop=True)

pt_enroll_visits = pt_enroll.merge(
    opt_visits_grouped, how='left', 
    left_on=['patient_id', 'month_year'], 
    right_on=['patient_id', 'opt_visit_month']
)

results = pt_enroll_visits.groupby(['patient_id', 'subgroup']).agg(
    patient_id = ('patient_id', 'first'),
    enrollment_start_date = ('month_year', 'min'),
    enrollment_end_date = ('month_year', 'max'),
    ct_outpatient_visits = ('ct_outpatient_visits', 'sum'),
    ct_days_with_outpatient_visit = ('ct_days_with_outpatient_visit', 'sum')
).reset_index(drop=True)

results.to_csv('results.csv', index=False)
print(results['ct_days_with_outpatient_visit'].nunique())
