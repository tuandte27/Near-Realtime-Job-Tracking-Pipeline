# <p align="center"> 🚀 Near Realtime Job Application Tracking Pipeline </p>

This project is a **near real-time data pipeline** (5-10 second interval) that tracks recruitment campaign performance. It uses a **Change Data Capture (CDC)** ETL process with **Apache Spark** to move and transform raw performance logs from **Cassandra** (Data Lake) to **MySQL** (Data Warehouse). The final goal is to display up-to-date metrics on **Grafana** dashboards for immediate monitoring and optimization of campaign bidding strategies. The entire system is containerized using **Docker** for easy deployment and orchestration

---

## 📌 Project Overview
The pipeline uses **Spark** and **Change Data Capture (CDC)** to process fresh data from source to reporting.

* **Extraction (CDC):** **Spark** pulls only new/updated event records from **Cassandra** (Data Lake) based on the timestamp (`<ts>`), using a **CDC** approach to target changes since the `<last_load>`.
* **Transformation:** **Spark** cleans, aggregates, and computes key performance metrics (`<clicks>`, `<applications>`, `<conversions>`), grouping by dimensions (like `<job_id>`, `<campaign_id>`) and enriching data by joining with company info from **MySQL**.
* **Loading:** The final, structured dataset is loaded into **MySQL** (Data Warehouse) in append mode.
* **Visualization:** **Grafana** reads the updated **MySQL** data to display near real-time performance dashboards.

---

## 🛠️ Tech Stack

| Component | Description |
| :--- | :--- |
| **Cassandra** | Serves as a **Data Lake** for storing raw, high-velocity data. |
| **Apache Spark** | Performs the **ETL processing** and transforms data for analysis. |
| **MySQL** | Acts as a **Data Warehouse** for structured storage and querying. |
| **Grafana** | Provides **near real-time dashboards** and visualizations. |
| **Docker** | **Containerizes** the entire system for easy deployment and orchestration. |

## 🗂️ Project Structure

```plaintext
Near-Realtime-Job-Application-Tracking-Pipeline/
├──jars
│   ├── mysql-connector-j-8.0.33.jar
│   └── spark-cassandra-connector-assembly_2.12-3.4.0.jar
├──.gitignore
├──docker-compose.yml
├──Dockerfile
├──ETL_script.py                                              # ETL file
├──README.md                                                  # Project documentation                                         
└── report.pdf                                                # Project report
```

---

## 🔄 ETL Flow

This pipeline extracts, transforms, and loads recruitment campaign performance data from **Cassandra** to **MySQL**, supporting near real-time monitoring on **Grafana**.

### 1. Extract
 - Data is pulled from **Cassandra**, which serves as the **data lake** storing raw event logs.
- Using **Apache Spark**, only new or updated records since the last successful MySQL load are retrieved, based on the **timestamp (`ts`)**.
### 2. Transform
- **Spark** processes raw event logs by:
  - Filtering and aggregating records.
  - Computing key metrics such as:
    - `clicks`
    - `qualified` / `unqualified applications`
    - `conversions`
- Data cleaning steps:
  - Filling null values
  - Grouping by relevant dimensions:
    - `job_id`, `campaign_id`, `publisher_id`, `group_id`, `date`, `hour`
  - Enriching with **company info** from MySQL
- The output is a **structured, analytics-ready dataset**.
### 3. Load
The transformed dataset is loaded into **MySQL** (data warehouse) in **append mode**.
- Each record contains:
  - Aggregated metrics
  - Timestamps
- This enables **near real-time dashboards** (e.g., Grafana) to display up-to-date performance metrics without delay.

### ⚡ Highlights
- **Near real-time updates**: 5–10 second intervals from **Cassandra** to **Grafana**  
- **CDC-based ETL** ensures only incremental changes are processed  
- **Containerized with Docker** for easy deployment and management  
- **Analytics-ready data** supports instant monitoring and optimization

![Flowchart](images/flowchart.png)

---

## 📈 Data Schemas (MySQL Output)

| Column | Data Type | Description |
| :--- | :--- | :--- |
| **id** | INT | Surrogate Key. A unique, auto-generated ID for the record (internal tracking). |
| **job_id** | INT | The unique identifier for the job or recruitment posting. |
| **dates** | DATE | The date on which this performance data was recorded. |
| **hours** | INT | The hour block during which this performance data was recorded. |
| **disqualified_application** | INT | The number of applications that were disqualified/did not meet the minimum requirements. |
| **qualified_application** | INT | The number of applications that were deemed qualified after initial screening. |
| **conversion** | INT | The number of candidates that resulted in a final conversion (e.g., successful hire). |
| **company_id** | INT | The unique identifier of the company owning the job posting. |
| **group_id** | INT | The identifier for the group or department related to the job posting (often used for classification or aggregation). |
| **campaign_id** | INT | The unique identifier for the recruitment advertising/marketing campaign. |
| **publisher_id** | INT | The identifier for the advertising vendor or source (e.g., Google Ads, Job Board A). |
| **bid_set** | DOUBLE | The amount of money paid for each click generated by the campaign or job posting. |
| **clicks** | INT | The total number of times users clicked on the ad or job posting. |
| **spend_hour** | DOUBLE | The total cost spent on the campaign or job posting in 1 hour. |
| **sources** | VARCHAR | The technical origin of the data (e.g., Cassandra, Log File). |
| **updated_at** | TIMESTAMP | The timestamp indicating the last time this record was updated in the system. |

### Sample Output:

| id | job_id | dates | hours | disqualified_application | qualified_application | conversion | company_id | group_id | campaign_id | publisher_id | bid_set | clicks | spend_hour | sources | updated_at |
| :--- | :--- | :--- | :--- | :--- | :--- | :--- | :--- | :--- | :--- | :--- | :--- | :--- | :--- | :--- | :--- |
| 1001 | 2050 | 2025-10-01 | 10 | 5 | 20 | 3 | 1 | 45 | 301 | 15 | 0.50 | 120 | 60.00 | Cassandra | 2025-10-01 11:00:00 |
| 1002 | 2051 | 2025-10-01 | 11 | 2 | 10 | 0 | 1 | 45 | 302 | 15 | 0.00 | 350 | 0.00 | Cassandra | 2025-10-01 12:00:00 |
| 1003 | 2052 | 2025-10-02 | 14 | 0 | 5 | 1 | 2 | 50 | 405 | 22 | 1.25 | 40 | 50.00 | Cassandra | 2025-10-02 15:00:00 |
| 1004 | 2053 | 2025-10-02 | 15 | 15 | 50 | 5 | 1 | 48 | 301 | 15 | 0.75 | 250 | 187.50 | Cassandra | 2025-10-02 16:00:00 |

---

## 📊 Grafana Dashboard

![dashboard](images/dashboard_grafana.png)

---

## 📞 Contact

* 👨‍💻 Author: Phan Anh Tuan
* 📧 Email: tuandte27@gmail.com
* 🐙 GitHub: [tuandte27](https://github.com/tuandte27)