-- Create the departments table
CREATE TABLE departments (
    id INTEGER PRIMARY KEY,
    department VARCHAR(100) NOT NULL
);

-- Create the jobs table
CREATE TABLE jobs (
    id INTEGER PRIMARY KEY,
    job VARCHAR(100) NOT NULL
);

-- Create the hired_employees table
CREATE TABLE hired_employees (
    id INTEGER PRIMARY KEY,
    name VARCHAR(100) NULL,
    datetime TIMESTAMP NULL,
    department_id INTEGER NULL,
    job_id INTEGER NULL
);






