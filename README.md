# loop11-data-converter
Converts a Loop11 data set (CSV) into an SQLite database

## Dependencies
dateparser

Install with: `pip install dateparser`

See docs for more info: http://dateparser.readthedocs.io/en/latest/

## Usage
`convert-loop11-csv.py --csv=[file] [--db==[file]]`

csv is the Loop11 data, db is output database name

## Database structure

All data from the CSV file is loaded into 5 tables:
  - participants
  - questions
  - tasks
  - question_response
  - task_response

participants are joined to questions and tasks via the *response tables
