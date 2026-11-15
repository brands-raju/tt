
-- Practical 9
-- Name: Vicky Jaiswal
-- RNO: 44030726024
-- Course: MSC.CS Part 1
-- Subject: BDA & SP

CREATE TABLE university.students (
    student_id INT PRIMARY KEY,
    student_name VARCHAR(100),
    department VARCHAR(100)
);

INSERT INTO university.students VALUES
(101, 'Aarav', 'Data Science'),
(102, 'Diya', 'Computer Science'),
(103, 'Rohan', 'Data Science'),
(104, 'Meera', 'Computer Science');

CREATE USER IF NOT EXISTS 'trino_user'@'%'
IDENTIFIED BY 'trino123';

GRANT SELECT ON university.* TO 'trino_user'@'%';
-- Aim: To use Trino to query data from two different sources (MySQL and S3) simultaneously.
-- What are we doing?
-- 1. MySQL: Stores student information like ID, name and department.
-- 2. S3 (MinIO): Stores course information.
-- 3. Trino: Acts as a query engine that connects both sources and retrieves data.
-- 4. Docker: Runs all these services in separate containers.
-- 5. JOIN: Combines student data from MySQL and course data from S3 using student ID.