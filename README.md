# Data Collection & Storage Foundation

## ❓ Problem Description
Based on the given mongodb-based database with the Consumer behavior of GLAMIRA
- Collecting, storing, and processing data including IP address and product information by using **Google Cloud Platform (GCP)**, **MongoDB**, and **Python**.
- The project also involves IP geolocation enrichment, product data crawling.


## ⏳ Duration
**July 14th, 2025 - July 27th, 2025**


## 🎯 Learning Objectives
- Understand cloud infrastructure setup with GCP.
- Work with **Google Cloud Storage (GCS)** and **VM instances**.
- Perform basic data processing using **Python**.
- Load and query data with **MongoDB**.
- Implement **IP geolocation** using `ip2location-python` library.
- Crawl product information.
- Conduct basic **data profiling** and documentation.


## 📋 Preparation

### **1. Environment Setup**
- Create a **GCP account** and project.
- Understand basic GCP concepts: projects, billing, IAM.
- Download **Glamira dataset** via [Onedrive](https://onedrive.live.com/?id=5961F7334E952FE9%21s6defb21b3a154eeea02046b6e39eae3b&cid=5961F7334E952FE9&redeem=aHR0cHM6Ly8xZHJ2Lm1zL2YvYy81OTYxZjczMzRlOTUyZmU5L0VodXk3MjBWT3U1T29DQkd0dU9lcmpzQi1paU9jcG10dlg0cTBxbUNzVk4ta3c%5FZT01OmVCbTlvYSZzaGFyaW5ndjI9dHJ1ZSZmcm9tU2hhcmU9dHJ1ZSZhdD05).


### **2. Access management**
- Access IAM on GCP
  => Service account
  => Create service account
  => Set service account ID
  => Set permission for the service account - should add role owner for all permission
  => Choose Action => Manage keys => Add key => Create new key => json file to download
- Reference to connect python with VM GCP: [Stackoverflow](https://stackoverflow.com/questions/51554341/google-auth-exceptions-defaultcredentialserror)


### **3. Google Cloud Storage Setup**
- Create **GCS bucket** for raw data.
- Understand storage classes.
- Configure authentication and service accounts.
- Upload raw dataset to the bucket.

### **4. Virtual Machine and Mongodb Setup**
- Create a **VM instance** in GCP.
- Install **MongoDB** on the VM. Reference [Mongodb](https://www.mongodb.com/docs/manual/tutorial/install-mongodb-on-ubuntu/)
- Test database connectivity.
- Configure VM GCP
  - Create network tag for VM
  - Create new firewall rule for TCP 27017 => choose specific network tag VM => set specific IP address to connect with Mongodb through VM GCP (prefer /32 to ensure the security)
- Configure Mongodb on VM
  - sudo vim /etc/mongod.conf
  - Change bindIP into 0.0.0.0
  - security
    authorization: enabled
- Create user and password for mongodb
  - mongosh
  - use admin
  - db.createUser({
    user: "user_name",
    pwd: "password",
    roles: [ { role: "readWrite", db: "database_name" } ]
  })

### **5. Initial Data Loading**
- Import raw dataset into **MongoDB**.
  - Transfer data from GCS bucket to VM instance: `sudo gsutil -m cp gs://file_path destination_path`
  - Restore bson file, which is already downloaded from preparation 1
- Run basic MongoDB queries to explore data.
- Document the data structure:
  - **Collections and fields**.
  - **Data dictionary**.

## 📝 Tasks
### **1. IP Location Processing**
- Collect all IP addresses interacting with GLAMIRA website
- Install `ip2location-python`.
- Write a Python script to:
  1. Connect to MongoDB.
  2. Retrieve unique IPs from the main collection.
  3. Use **IP2Location** to enrich data with geolocation.
  4. Save results into a new collection.

### **2. Product Data Processing**
- Filter events (collection in the document in Mongodb database) and extract `product_id` (or `viewing_product_id`) and `current_url` for
  `view_product_detail`, `select_product_option`, `select_product_option_quality`,
  `add_to_cart_action`, `product_detail_recommendation_visible`,
  `product_detail_recommendation_noticed`.
- Filter events and extract `viewing_product_id` and `referrer_url` for `product_view_all_recommend_clicked`
- For collection product_view_all_recommend_clicked, creating index
```db.summary.createIndex({ collection: 1, viewing_product_id: 1, referrer_url: 1 })```
- For remaining collection, creating index
```db.summary.createIndex({ collection: 1, product_id: 1, current_url: 1 })```
```db.summary.createIndex({ collection: 1, viewing_product_id: 1, current_url: 1 })```
- Write a Python script to crawl the **product information** using `referrer_url` or `current_url`

**2.1 Filter event and extract product_id, url**
- Group by pid and url and get all.
- Then create set() and assign urls corresponding to pid.
- Convert urls to list and save to json file

**2.2 Crawling product information based on pid, url and domain**
- Crawl all the information of different links and save it to json as tags
- For example: pid: 10000, "com": ....................., "co.uk": ....................

**Note: Improve performance for crawling data**

**Merge pid and url from filters**
- Cover all pid and url into one file:
  - Collect unique url through all filters
  - If there exists "checkout" in url, it will be removed
  - If Subdomain of url is not "www", it will be removed

**Select appropriate approach for crawling**
- Option 1: Sequencing: the lowest approach
- Option 2: Multiprocessing: faster than sequencing, but puts a burden on the CPU and, and is prone to IP blocking
- Option 3: multiprocessing combined with Multithreading and Proxies: High performance, but it is exorbitant for high quality rotating residential proxies
- Option 4: Asynchronous: Fast, stable, rarely blocked IP with suitable semaphore

  => In this project, its better to use Async with the package product_enrichment

**Collecting product data for an url**
- With 404 error url, return error and stop retrying
- With 403 forbidden error url, return error and stop retrying
- With 429 error url due to too many requests, wait with random seconds and retry, after 3 attempts, return error

**Collecting product data for a product_id**
  - Collect errors within 10s, after 10s or when there are 50 errors, save errors once
  - When a product_id with an url of a specific domain has been crawled, so the domain process function will return result and stop all other url. For example, if ".com" has been crawled, it return data and stop function for the domain ".com"

## 📝 How to set up and run
1. Create virtual environment and install required library in the file `requirements.txt`
2. In all environment folder, with file config.yml, add necessary content
    - MONGODB_URL: format string: mongodb://<user_name>:<password>@<external_IP_of_VM>:<mongodb_port>/
    - CREDENTIAL: the json file name of the key of the service account
3. Create folder behavior_data and add the data that we already downloaded via Onedrive
4. Upload behavior_data into GCS and transfer to VM, restore it on MongoDB VM GCP
5. `cd data_enrichment` and run package `location_enrichment`
6. `cd data_enrichment` and run package `product_enrichment`
7. Run package data_profiling
8. Run package upload_gcs

## Project Structure Files
- `crawl_checker`: used to check product ids that already crawled, uncrawled
- `data`: contains final data including ip2location data, raw data, product data in jsonl file  (automatically generated when running package upload_gcs)
- `data_enrichment`: enrich data with location and product information
  - `checkpoint`: contains checkpoint during collecting ip address from raw data, crawling product data and convert ip to location (automatically generated when running code)
  - `data`: (automatically generated when running code): contains errors, results
  - `database`: contains IP2LOCATION BIN file used to convert IP to location, after downloading via Onedrive, put it here
  - `environment/config.yml`: set up environment including service account file, mongodb url,...
  - `location_enrichment`: used to enrich location from ip address
  - `product_enrichment`: used to enrich location from product id and urls
  - `logs/data_enrichment.log`: save log during running code for data_enrichment
- `data_profiling`: used to analyze data after collecting including product data and ip2location data
- `environment/config.yml`: set up environment including service account file, mongodb url,...
- `logs/mainlog.log`: save log during running code for data_profiling, upload_gcs,...
- `upload_gcs`: used to download raw, product and ip2location data from MongoDB, then upload into GCS

# Project 7
install dbt-core, dbt-snowflake, dbt-bigquery