CREATE DATABASE DB1
USE MODEL
GO
CREATE TABLE T1 (ID INT, NAME VARCHAR)
INSERT INTO T1 VALUES (1, 'T');

USE MASTER
GO
CREATE DATABASE DB2

SELECT @@VERSION

--Microsoft SQL Server 2017 (RTM) - 14.0.1000.169 (X64)  
--Aug 22 2017 17:04:49   Copyright (C) 2017 Microsoft Corporation  Enterprise Edition (64-bit) on Windows 10 Pro 10.0 <X64> (Build 16299: ) 

USE DB2
GO
CREATE TABLE T3 (ID INT, NAME VARCHAR) on fg1
INSERT INTO T3 VALUES (1, 'T');
