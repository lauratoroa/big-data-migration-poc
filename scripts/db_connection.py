import boto3
import json
import psycopg2

def get_secret():
    secret_name = "big-data-migration-secrets"
    region_name = "us-east-2"

    # Create the client
    client = boto3.client("secretsmanager", region_name=region_name)

    # Get the secret
    response = client.get_secret_value(SecretId=secret_name)
    secret = json.loads(response["SecretString"])

    return secret

def test_db_connection():
    secrets = get_secret()
    
    try:
        conn = psycopg2.connect(
            dbname=secrets["DB_NAME"],
            user=secrets["DB_USER"],
            password=secrets["DB_PASSWORD"],
            host=secrets["DB_HOST"],
            port=secrets["DB_PORT"]
        )
        
        # verify connection
        cur = conn.cursor()
        cur.execute("SELECT version();")
        db_version = cur.fetchone()
        print(f"Connection successful! Database version: {db_version[0]}")
        
        # close connection
        cur.close()
        conn.close()
    
    except Exception as e:
        print(f"Connection failed: {e}")

# Execute test
test_db_connection()
