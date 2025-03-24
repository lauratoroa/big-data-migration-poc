# Big Data Migration PoC

## Challenge #1  
You are a data engineer at Globant and you are about to start an important project.  
This project involves big data migration to a new database system. You need to create a PoC to solve the following requirements:

### Requirements:
1. **Move historic data** from CSV files to the new database.  
2. **Create a REST API** to receive new data:  
   - 2.1. Each new transaction must fit the data dictionary rules.  
   - 2.2. Support batch inserts (1 up to 1000 rows per request).  
   - 2.3. Receive the data for all tables in the same service.  
   - 2.4. Enforce data validation rules for each table.  
3. **Create a backup feature** to save each table in AVRO format.  
4. **Create a restore feature** to recover tables from AVRO backups.  
5. **Frequent commits to GitHub** to track development progress.  

### Clarifications:
- You decide the **source location** of the CSV files.  
- You choose the **SQL database** type.  
- The CSV files are **comma-separated**.  
- "Feature" can be a **REST API, Stored Procedure, Database Functionality, Cron Job,** etc.  

### Not mandatory, but taken into account:
✔ Create a **`README.md`** file.  
✔ Security considerations for the API.  
✔ Use **Git workflow** for versioning.  
✔ Create a **Dockerfile** for deployment.  
✔ Use **cloud tools** instead of local tools.  

### Allowed Languages:
- Python   
- Java   
- Go 
- Scala 

---

## Data Rules
- Transactions that do not meet the rules **must not be inserted**, but they must be **logged**.  
- **All fields are required**.  

### CSV File Structures:  

#### **`hired_employees.csv`**  
| Column         | Type    | Description |
|---------------|--------|-------------|
| `id`          | INTEGER | Employee ID |
| `name`        | STRING  | Employee's full name |
| `datetime`    | STRING  | Hire datetime (ISO format) |
| `department_id` | INTEGER | ID of the department where the employee was hired |
| `job_id`      | INTEGER | ID of the job the employee was hired for |

**Example Data:**  

4535,Marcelo Gonzalez,2021-07-27T16:02:08Z,1,2
4572,Lidia Mendez,2021-07-27T19:04:09Z,1,2
Link to the file

#### **`departments.csv`**  
| Column | Type    | Description |
|--------|--------|-------------|
| `id`   | INTEGER | Department ID |
| `department` | STRING | Department Name |

**Example Data:**  

1, Supply Chain
2, Maintenance
3, Staff

#### **`jobs.csv`**  
| Column | Type    | Description |
|--------|--------|-------------|
| `id`   | INTEGER | Job ID |
| `job`  | STRING  | Job Title |

**Example Data:**  

1, Recruiter
2, Manager
3, Analyst

## Challenge #2  

You need to explore the data that was inserted in the first challenge. The stakeholders ask for some specific metrics they need. You should create an endpoint for each requirement.  

### Requirements  

1. **Number of employees hired for each job and department in 2021 divided by quarter.**  
   The table must be ordered alphabetically by department and job.  

   **Output example:**  

   | department    | job       | Q1 | Q2 | Q3 | Q4 |
   |--------------|----------|----|----|----|----|
   | Staff        | Recruiter | 3  | 0  | 7  | 11 |
   | Staff        | Manager   | 2  | 1  | 0  | 2  |
   | Supply Chain | Manager   | 0  | 1  | 3  | 0  |

2. **List of IDs, name, and number of employees hired for each department that hired more employees than the mean of employees hired in 2021 for all departments.**  
   The list should be ordered by the number of employees hired (descending).  

   **Output example:**  

   | id  | department     | hired |
   |----|--------------|-------|
   | 1  | Staff        | 45    |
   | 2  | Supply Chain | 12    |

### Not mandatory, but taken into account:  
- Create a visual report for each requirement using your favorite tool.