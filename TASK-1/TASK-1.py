import pandas as pd

# Specify the full path to the CSV file
path = r'C:\Users\Axioo Pongo\Documents\Alterra\ingestion-data\dataset\yellow_tripdata_2020-07.csv'
df = pd.read_csv(path, sep=',')

df_rename = df.rename(columns= {"VendorID":"Vendor_ID","RatecodeID":"Rate_Code_ID",
                                "PULocationID":"PULocation_ID",
                                "DOLocationID":"DOLocation_ID"})

bottom_10_passenger = df_rename.nsmallest(10, 'passenger_count') [['Vendor_ID', 'passenger_count', 'trip_distance', 'payment_type', 'fare_amount', 'extra', 'mta_tax', 'tip_amount', 
                     'tolls_amount', 'improvement_surcharge', 'total_amount', 'congestion_surcharge']]

print(bottom_10_passenger)

