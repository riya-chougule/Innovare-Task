# Innovare-Task

## Setup

1. Create virtual environment:

```bash
   python3 -m venv myenv
   source myenv/bin/activate
```
2. Install dependencies:
```bash
pip install -r requirements.txt 
```

3. Place CSV files in data/ folder.
4. Architectural flow of python script - 

   <img width="1409" alt="Architectural Flow.jpeg" src="Architectural Flow.jpeg">

  ## 🔄 Script Flow

a. **Data Ingestion**  
   - Reads three CSV files:  
     - `student_demographics.csv`  
     - `gradebook_export.csv`  
     - `attendance_records.csv`  
   - Handles missing files or structural errors gracefully.

b. **Data Cleaning**  
   - Standardizes formats (dates, IDs, categorical values).  
   - Handles missing or invalid values (e.g., wrong grades, blank fields).  
   - Normalizes identifiers across datasets.

c. **Data Transformation**  
   - Extracts and standardizes start dates.  
   - Converts grades to numeric values.  
   - Normalizes `student_id` formats.  
   - Prepares unified schema for merging.

d. **Data Quality Checks**  
   - Identifies and removes duplicates.  
   - Verifies column consistency.  
   - Logs warnings for anomalies.

e. **BigQuery Load**  
   - Loads cleaned & transformed data into a **BigQuery table**:  
     `fot_student_features`.

f. **Feature Engineering (BigQuery)**  
   - Executes SQL to calculate for each student:  
     - `credits_earned_semester`  
     - `core_course_failures`  
     - `attendance_percentage`  
     - `behavioral_flags`  
   - Saves final results in BigQuery for downstream use.

g. **Logging & Error Handling**  
   - Detailed logs for every stage.  
   - Errors are caught and reported without halting earlier stages unnecessarily.




6. Run pipeline:   
```bash
python src/main.py
```
5. Logs are saved in logs/app.log
