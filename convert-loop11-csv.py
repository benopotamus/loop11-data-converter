#!/usr/bin/env python3

import csv, re, sqlite3, sys
from os.path import isfile
import dateparser #http://dateparser.readthedocs.io/en/latest/


def main():
	conn = sqlite3.connect(DB_NAME)
	c = conn.cursor()

	# Drop tables if file exists
	if isfile(DB_NAME):
		c.execute('''DROP TABLE participants''')
		c.execute('''DROP TABLE tasks''')
		c.execute('''DROP TABLE questions''')
		c.execute('''DROP TABLE task_response''')
		c.execute('''DROP TABLE question_response''')

	# Create table
	c.execute('''CREATE TABLE participants (
				number INTEGER,
				customid TEXT,
				ipaddress TEXT,
				xl_date_start STRING,
				xl_date_end STRING,
				sqlite_date_start STRING,
				sqlite_date_end STRING,
				user_agent TEXT,
				total_time REAL,
				avg_time REAL,
				ave_page_views REAL
				)''')

	c.execute('''CREATE TABLE tasks (
				number INTEGER,
				name TEXT
				)''')

	c.execute('''CREATE TABLE questions (
				main TEXT,
				sub TEXT
				)''')

	c.execute('''CREATE TABLE task_response (
				participantid INTEGER REFERENCES participants(rowid) ON UPDATE CASCADE ON DELETE CASCADE,
				questionid INTEGER	REFERENCES tasks(rowid) ON UPDATE CASCADE ON DELETE CASCADE,
				result TEXT,
				page_views INTEGER,
				time INTEGER
				)''')

	c.execute('''CREATE TABLE question_response (
				participantid INTEGER REFERENCES participants(rowid) ON UPDATE CASCADE ON DELETE CASCADE,
				questionid INTEGER	REFERENCES questions(rowid) ON UPDATE CASCADE ON DELETE CASCADE,
				answer TEXT
				)''')

	conn.commit()



	raw_data = []
	with open(FILE_NAME) as csvfile:
		reader = csv.reader(csvfile)

		for row in reader:
			raw_data.append(row)



	def get_exceltime(dt):
		'''Returns a date format for exporting to excel spreadsheets

		It's just the original datetime minus the comma (because the export format is CSV)'''
		return dt.replace(',','')

	def get_timestamp(dt):
		'''Returns a datetime in iso format

		2016-03-09T13:36:00

		Intended usage is sorting and filtering using SQL queries'''
		return dateparser.parse(dt).isoformat()



	# Work out which row the column headers / questions start on
	# Determine sub-question header row based on this too
	for row_number, row_value in enumerate(raw_data):
		if row_value[0] == 'Participant No.':
			qheaders = raw_data[row_number]
			sqheaders = raw_data[row_number+1]
			break


	# Determine which columns have the tasks and questions
	# may be multiple columns per object

	ptask = re.compile(r'Task ([0-9]+): ')
	tasks = []
	questions = []

	# Start from the 10th column becaue the first 9 are the participants details
	i = 9
	while i < len(qheaders):
		column = qheaders[i]

		# If this is a task column - get the values
		#
		# Example: 1. Task. Linkages Task 1: Find the definition

		if '. Task. ' in column:

			match = ptask.search(column)

			# A match should always exist
			if match:
				tasks.append({
					'name': column[match.end():],
					'number': match.group(1),
					'pos': i  # the column position
				})

				# skip the next 2 columns as they contain task values
				i = i + 3

			else:
				print('Error: I thought I found a task but it seems I did not')
				i = i + 1

		else:
			# Must be a question
			# Some questions have multiple columns of answers
			# pos is the column number the question starts on, endpos is where it ends (to allow for multi-common questions)
			# We initially set the endpos for every question as the last column in the data,
			#   and rely on the last_question code to overwrite it (it won't overrwite the last column)
			if column is not '':

				questions.append({
					'name': column,
					'pos': i,
					'endpos': len(qheaders),
					'subs': []
				})

				# The previous questions end position is the pos before this one's start
				# Set that end position
				# Try statement handles when this is the first question
				try:
					last_question = questions[-2]
					last_question['endpos'] = i
				except IndexError:
					pass

			i = i + 1


	# Get sub-questions by using start and end positions for questions to pick the headings out of the original data set
	for question in questions:
		for subq in range(question['pos'], question['endpos']):
			question['subs'].append({
				'name': sqheaders[subq],
				'pos': subq
			})


	### Create tasks and questions in db
	for task in tasks:
		c.execute('INSERT INTO tasks VALUES (?,?)', (task['number'], task['name']))
		task['rowid'] = c.lastrowid

	# The main object for questions is really the subs
	# The parent question just provides additional context for the sub
	for question in questions:
		for sub in question['subs']:
			c.execute('INSERT INTO questions VALUES (?,?)', (question['name'], sub['name']))
			sub['rowid'] = c.lastrowid

	conn.commit()




	### Work over list of rows, creating participants and associated tasks and questions responses

	# Starting from the 6th row (the stuff above that is the spreadsheet headers)
	for row in raw_data[5:]:

		# Rows 0-8 are the participant details
		#
		# Participant
		# No.CustomID
		# IP Address
		# Date Started (x2 one for XL and one for SQL)
		# Date Completed (x2 one for XL and one for SQL)
		# User Agent
		# Total Time Spent
		# Avg Time Taken
		# Avg Page Views
		c.execute('INSERT INTO participants VALUES (?,?,?,?,?,?,?,?,?,?,?)', (
			row[0][12:],
			row[1],
			row[2],
			get_exceltime(row[3]),
			get_exceltime(row[4]),
			get_timestamp(row[3]),
			get_timestamp(row[4]),
			row[5],
			row[6],
			row[7],
			row[8]
		))

		participant_rowid = c.lastrowid
		conn.commit()

		# Now that we have the participant values, the rest are tasks and questions.
		for task in tasks:
			c.execute('INSERT INTO task_response VALUES (?,?,?,?,?)', (
				participant_rowid,
				task['rowid'],
				row[task['pos']],
				row[task['pos']+1],
				row[task['pos']+2]
			))

		for question in questions:
			for sub in question['subs']:
				c.execute('INSERT INTO question_response VALUES (?,?,?)', (
					participant_rowid,
					sub['rowid'],
					row[sub['pos']]
				))

	conn.commit()


if __name__ == '__main__':

	help = False
	FILE_NAME = False
	DB_NAME = False

	for arg in sys.argv:
		if arg.startswith('--db'):
			DB_NAME = arg.replace('--db=','')
		elif arg.startswith('--csv'):
			FILE_NAME = arg.replace('--csv=','')
		elif arg.startswith('--help'):
			help = True


	# If file name wasn't passed as an argument, maybe it was just entered with no switch
	if not FILE_NAME:
		try:
			if len(sys.argv) == 2:
				FILE_NAME = sys.argv[1]
		except NameError:
			print('usage: convert-loop11-csv.py --csv=[file] [--db==[file]]\n\ncsv is input, db is output database\n')


	if help or len(sys.argv) < 2:
		print('usage: convert-loop11-csv.py --csv=[file] [--db==[file]]\n\ncsv is input, db is output database\n')
	else:

		# If filename has been worked out, but not the db name, make the db name the same as file name
		if FILE_NAME and not DB_NAME:
			DB_NAME = FILE_NAME[:-4] + '.db'

		main()
