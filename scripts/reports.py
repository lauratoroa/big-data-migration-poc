import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns
from sqlalchemy import create_engine
import json
import boto3

# Get Database Credentials
def get_secret():
    secret_name = "big-data-migration-secrets"
    region_name = "us-east-2"
    client = boto3.client("secretsmanager", region_name=region_name)
    response = client.get_secret_value(SecretId=secret_name)
    secret = json.loads(response["SecretString"])
    return secret

secrets = get_secret()
DATABASE_URL = f"postgresql://{secrets['DB_USER']}:{secrets['DB_PASSWORD']}@{secrets['DB_HOST']}:{secrets['DB_PORT']}/{secrets['DB_NAME']}"
engine = create_engine(DATABASE_URL)

# Query 1: Number of employees hired for each job and department in 2021 divided by quarter. 
#          The table must be ordered alphabetically by department and job.

query_1 = """
SELECT d.department, j.job,
   COUNT(*) FILTER (WHERE EXTRACT(QUARTER FROM he.datetime) = 1) AS Q1,
   COUNT(*) FILTER (WHERE EXTRACT(QUARTER FROM he.datetime) = 2) AS Q2,
   COUNT(*) FILTER (WHERE EXTRACT(QUARTER FROM he.datetime) = 3) AS Q3,
   COUNT(*) FILTER (WHERE EXTRACT(QUARTER FROM he.datetime) = 4) AS Q4
FROM hired_employees he
JOIN departments d ON he.department_id = d.id
JOIN jobs j ON he.job_id = j.id
WHERE EXTRACT(YEAR FROM he.datetime) = 2021
GROUP BY d.department, j.job
ORDER BY d.department, j.job;
"""

df1 = pd.read_sql(query_1, engine)

#  Visualization
df1_melted = df1.melt(id_vars=["department", "job"], var_name="Quarter", value_name="Hires")

plt.figure(figsize=(12, 6))
sns.barplot(x="Quarter", y="Hires", hue="department", data=df1_melted)
plt.title("Employees Hired per Job & Department (2021) by Quarter")
plt.xticks(rotation=45)
plt.legend(title="Department", bbox_to_anchor=(1, 1))
plt.show()

# Query 2: List of ids, name and number of employees hired of each department that hired more
#       employees than the mean of employees hired in 2021 for all the departments, ordered
#       by the number of employees hired (descending).

query_2 = """
WITH department_hires AS (
        SELECT he.department_id, d.department, COUNT(*) AS hires
        FROM hired_employees he
        JOIN departments d ON he.department_id = d.id
        WHERE EXTRACT(YEAR FROM he.datetime) = 2021
        GROUP BY he.department_id, d.department
    ),
    department_avg AS (
        SELECT AVG(hires) AS avg_hires 
		FROM department_hires
    )
    SELECT dh.department_id, dh.department, dh.hires
    FROM department_hires dh
    JOIN department_avg da ON dh.hires > da.avg_hires
    ORDER BY dh.hires DESC;
"""

df2 = pd.read_sql(query_2, engine)

#  Visualization
plt.figure(figsize=(10, 5))
sns.barplot(y=df2["department"], x=df2["hires"], palette="coolwarm")
plt.title("Departments Hiring Above Average in 2021")
plt.xlabel("Number of Employees Hired")
plt.ylabel("Department")
plt.show()
