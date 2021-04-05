CREATE DATABASE MY_DB;

SELECT * FROM dbo.phi_CA;

ALTER TABLE dbo.phi_CA ALTER COLUMN [date] datetime2;


/* SELECTION/PROJECTION */

SELECT 
	[date], numconf, numdeaths, numtested, numrecover 
FROM 
	dbo.phi_CA 
WHERE 
	prname = 'Ontario'
ORDER BY 
	[date] DESC;

DELETE FROM dbo.phi_CA WHERE prname = 'Nunavut';

SELECT DISTINCT prname from dbo.phi_CA;

SELECT DISTINCT Province from dbo.phi_JH;

DELETE FROM dbo.phi_JH WHERE Province = 'Grand Princess';


/*FILTERING*/

SELECT DISTINCT prname FROM dbo.phi_CA WHERE prname LIKE '[OBQ]%';

SELECT 
	prname, [date], numtoday 
FROM 
	dbo.phi_CA 
WHERE 
	prname = 'Ontario' AND numtoday BETWEEN 900 AND 1400
ORDER BY
	[date];


/*JOINS*/

SELECT * FROM dbo.phi_JH ORDER BY [Date];

ALTER TABLE 
	dbo.phi_JH
ADD CONSTRAINT 
	PK_phi_JH_Date_Province 
PRIMARY KEY
	([Date], Province);

ALTER TABLE 
	dbo.phi_CA
ADD CONSTRAINT 
	FK_phi_CA_date_prname
FOREIGN KEY
	([date], prname)
REFERENCES 
	dbo.phi_JH ([Date], Province);

SELECT 
	dbo.phi_JH.[Date], Province, numtoday, numdeathstoday, numtestedtoday, numrecoveredtoday
FROM 
	dbo.phi_CA
INNER JOIN
	dbo.phi_JH
ON 
	dbo.phi_CA.[date] = dbo.phi_JH.[Date] AND dbo.phi_CA.prname = dbo.phi_JH.Province
ORDER BY
	Province, dbo.phi_JH.[Date];


/* AGGREGATIONS */

SELECT 
	prname AS 'Province/Territory', 
	SUM(numtoday) AS 'Confirmed Cases' 
FROM 
	dbo.phi_CA 
GROUP BY 
	prname
ORDER BY
	SUM(numtoday) DESC;

/* SORTING */

SELECT 
	prname AS 'Province/Territory', 
	[date] AS 'Date',
	numrecoveredtoday AS 'Number of Recoveries' 
FROM 
	dbo.phi_CA 
ORDER BY
	prname ASC,
	numrecoveredtoday DESC;
