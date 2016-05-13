#!/usr/bin/env python3

import csv, re, sqlite3, sys


def main():
	conn = sqlite3.connect(DB_NAME)
	c = conn.cursor()

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
		'''Returns a datetime in SQLite format (a specially formatted string)

		Intended usage is sorting and filtering using SQL queries'''
		dt = dt.split(', ')

		#t = dt[0].split(':')
		d = dt[1].split(' ')

		if d[1] == 'Jan':
			d[1] = 0
		elif d[1] == 'Feb':
			d[1] = 1
		elif d[1] == 'Mar':
			d[1] = 2
		elif d[1] == 'Apr':
			d[1] = 3
		elif d[1] == 'May':
			d[1] = 4
		elif d[1] == 'Jun':
			d[1] = 5
		elif d[1] == 'Jul':
			d[1] = 6
		elif d[1] == 'Aug':
			d[1] = 7
		elif d[1] == 'Sep':
			d[1] = 8
		elif d[1] == 'Oct':
			d[1] = 9
		elif d[1] == 'Nov':
			d[1] = 10
		elif d[1] == 'Dec':
			d[1] = 11

		d[1] = str(d[1])

		return '-'.join(d) + ' ' + dt[0]



	# Determine which columns have the tasks and questions
	# may be multiple columns per object

	ptask = re.compile(r'Task ([0-9]+): ')
	tasks = []
	questions = []

	# Start from the 10th column becaue the first 9 are the participants details
	i = 9
	while i < len(raw_data[3]):
		column = raw_data[3][i]

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
			if column is not '':

				questions.append({
					'name': column,
					'pos': i,

					# Every question has a 'Response' subquestion
					'subs': [{
						'name': 'Response',
						'pos': i
					}]
				})

			i = i + 1


	# Start from the 'pos' of the first question
	question_num = 0


	for index, column in enumerate(raw_data[4][questions[0]['pos']:], questions[0]['pos']):

		# Move onto next question every time we find a 'Response' column
		# This column is always the first subquestion for a question
		if column == 'Response':
			question_num = question_num + 1 # Next question!
		else:
			question = questions[question_num]

			question['subs'].append({
				'name': column,
				'pos': index
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
			print('usage: convert-loop11-csv.py --csv=[file] [--db==[file]]\n\ncsv is input, db is output database')


	if help or len(sys.argv) < 2:
		print('usage: convert-loop11-csv.py --csv=[file] [--db==[file]]\n\ncsv is input, db is output database')
	else:

		# If filename has been worked out, but not the db name, make the db name the same as file name
		if FILE_NAME and not DB_NAME:
			DB_NAME = FILE_NAME[:-4] + '.db'

		print(FILE_NAME)
		print(DB_NAME)

		main()