# Build Data Pipeline using Python

## Preparation
1. Active the virtual env with `python3 -m venv venv && source ./venv/bin/activate` (Recommended!)
2. Install depedencies with `pip install -r requirements.txt`
3. Create Google Service Account then name it `service-account.json`. Follow the guide [here](https://cloud.google.com/iam/docs/creating-managing-service-accounts) to create one

## How to
To run the ELT script, use:
```
python ingestion.py --type elt --to demo_pipeline_project.vix_stocks --service-account service-account.json
```
To run the ETL script, use:
```
python ingestion.py --type etl --to demo_pipeline_project.employees --service-account service-account.json
```

## How to Run the Script on schedule
1. Copy the `cron.sh` contents.
2. Type `crontab -e` on your terminal, then paste the content from no.1 there.<br>
See [Crontab Guru](https://crontab.guru/) to help you on write the proper cron syntax
3. Save the file.

## Using Docker to Run the Program
1. Build the image by
```
docker build -t <IMAGE_NAME>:<TAG> .
```
2. Run the container with
```
docker run --rm <IMAGE_NAME>:<TAG> --type {elt|etl} --to <DATASET_NAME>.<TABLE_NAME> --service-account service-account.json
```
3. Wait until the program run successfully