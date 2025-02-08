drop table jobs;
CREATE TABLE jobs (
    id INT PRIMARY KEY, 
    job VARCHAR(255) NOT NULL
);
drop table departments;
CREATE TABLE departments (
    id INT PRIMARY KEY, 
    department VARCHAR(255) NOT NULL
);
drop table hired_employees;
CREATE TABLE hired_employees (
    id INT PRIMARY KEY, 
    [name] VARCHAR(255) NOT NULL, 
    [datetime] VARCHAR(255) NOT NULL, 
    department_id INT NOT NULL, 
    job_id INT NOT NULL
);