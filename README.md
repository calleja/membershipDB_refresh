Resources:

from CIVI:
- sql reports

ETL COMPONENTS
- python file to process CIVI Select Activity Report (membership)
- python orchestration files to further process the database and prepare the 'Status' and 'Type' tables
- sql files (stored procedures)

PROJECT PLAN (input for Copilot Plan agent)
high level plan prompt: create a workflow or set of tasks that update the membership_db with data from one query to a production database. Each database resides in a separate digitial ocean droplet. Both databases are mysql. Consider the following tech stack: Digial Ocean droplet for hosting, github actions for scheduling and execution, python for project management and scripting, mysql as the database. If possible, the python portion should be managed as a python project using uv. Currently the query is in the form of an sql file. The workflow should include testing to ensure a sanity check on the result set from the query. Historical result sets should be used to formulate the tests.
