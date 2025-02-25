# Trustana - Data Engineer Case Study

## Description
An ETL pipeline which reads json data, cleans, standardizes & transforms it before engineering features that can be used by machine learning models to predict the confidence levels of Attributes. Also contains the Cloud Function code that will be used to automate the loading process into an OLAP store to efficiently retrieve features for model training & inference.

## Approach
- Performed cleaning & standardization post extraction across the dataset to maintain data integrity.
- Used natural language text processing techniques before vectorization & encoding to create features.
- Created an event-triggered pipeline to load features to a Data Lake which triggers the build & updation of the feature store on a Data Warehouse, while maintaining point-in-time feature retrieval to ensure consistency across training & serving.

## Flow
- `ET-features-gcs` contains code to load data into your Google Cloud Storage bucket (needs to be created by you in your GCP project).
- `gcs-store-bq` contains code that needs to be uploaded into Cloud Function, which creates a BigQuery table as the feature store.

## Setup `ET-features-gcs`
- Navigate to Google Cloud console.
- Enable APIs for Cloud Storage, BigQuery, Cloud Run Functions in your project.
- Navigate to IAM -> Service Accounts & download service account json key for authentication.
- Navigate to Compute Engine -> VM Instances -> Create Instance -> SSH -> Clone both repos
- Upload your service account json key into the cloned repository.
- Navigate to `app.py` -> TO-DO -> replace `service_account_key` & `bucket-name` with yours
- Build the Docker image: `sudo docker build -t ET-feature-gcs .`

## Setup `gcs-store-bq`
- Navigate to Cloud Run -> Write a Function -> choose Runtime `Python 3.10` -> Trigger -> Cloud Storage Trigger -> choose Event Provider as `Cloud Storage` -> choose Event Type as `google.cloud.storage.object.v1.finalized` -> Browse to your bucket which will stage the results of `ET-features-gcs` -> Save Trigger -> Create
- Replace all files in Inline Editor with files in `gcs-store-bq` cloned repo.
- Choose Base Image: `Python 3.10`
- Change Function entry point: `dataUpload`
- Deploy

## How to run
- Navigate to `ET-features-gcs` cloned repo.
- Run the app: `sudo docker run -p 5000:5000 ET-features-gcs`. You should now see the `feature_store.csv` in your GCS bucket which will trigger the Cloud Function.
- Navigate to BigQuery & on the left pane -> project_id -> dataset_id -> table_id
- Feature Store is created. If a new `feature_%XYZ.csv` arrives in bucket_name, the BQ table will get updated with new columns as features with a fresh `createdAt` timestamp.

## Dependencies
- Python 3.10
- Libraries: See `requirements.txt`
- Google Cloud account
