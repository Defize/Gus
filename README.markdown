Use Gus to run sql scripts against a database. Gus keeps a history of scripts which it has applied so only new scripts are run.


Some Command Line Examples:

**Apply sql scripts against your database**
*Gus.exe apply /source="C:\MyScripts" /server="." /database="AdventureWorks"*

**If scripts have already been applied manually you can use Gus to record the latest**
*Gus.exe apply /source="C:\MyScripts" /server="." /database="AdventureWorks" /recordonly=True*

**If using SQL Server Authentication you can specify a username and password**
*Gus.exe apply /source="C:\MyScripts" /server="MyServer" /username="Admin" /password="pa$$word" /database="AdventureWorks"*

**List the SQL scripts not yet applied**
*Gus.exe status /source="C:\MyScripts" /server="." /database="AdventureWorks"*

