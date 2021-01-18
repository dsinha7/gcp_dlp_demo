### Readme 
This is a simple example to De-identify a csv file using DLP API.  
This sample can run in any idea, but required GCP project with DLP service enabled 

#### Enable DLP fot the project 
    
    gcloud services enable dlp.googleapis.com
    
#### Change the project id and run following in cloud shell
    export PROJECT_ID=[dlp-demo-302116]
    export SERVICE_AC=glp-owner
    gcloud iam service-accounts create ${SERVICE_AC}
    gcloud projects add-iam-policy-binding ${PROJECT_ID} \
    --member="serviceAccount:${SERVICE_AC}@${PROJECT_ID}.iam.gserviceaccount.com" --role="roles/owner"
    
    gcloud iam service-accounts keys create dlp_key.json \
    --iam-account=${SERVICE_AC}@${PROJECT_ID}.iam.gserviceaccount.com
note : own role should not be granted in production setting  

download the dlp key and set the environment variable in IDE or shell
GOOGLE_APPLICATION_CREDENTIALS=[path to the dlp_key.json

