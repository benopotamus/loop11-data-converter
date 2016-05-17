#!/usr/bin/env python3

import csv, sqlite3, sys


def main():

	conn = sqlite3.connect(DB_NAME)
	c = conn.cursor()

	try:
		c.execute('''ALTER TABLE participants ADD usertype STRING''')
		conn.commit()
	except sqlite3.OperationalError:
		# Above SQL errors if the column already exists
		print('usertype column already exists in table participants')


	with open(FILE_NAME) as csvfile:
		reader = csv.reader(csvfile)

		for row in reader:
			c.execute('UPDATE participants SET usertype=? WHERE number=?', (row[1], row[0].split(' ')[1]))

	conn.commit()


if __name__ == '__main__':

	help = False

	for arg in sys.argv:
		if arg.startswith('--db'):
			DB_NAME = arg.replace('--db=', '')
		elif arg.startswith('--csv'):
			FILE_NAME = arg.replace('--csv=', '')
		elif arg.startswith('--help'):
			help = True

	try:
		if len(sys.argv) == 2:
			FILE_NAME = sys.argv[1]
			DB_NAME = FILE_NAME[:-3] + '.db'
	except NameError:
		print('usage: add-usertype-to-db.py --csv=[file] --db==[file]\n\ncsv is the extra data to add\n')

	if help or len(sys.argv) < 2:
		print('usage: add-usertype-to-db.py --csv=[file] --db==[file]\n\ncsv is the extra data to add\n')
	else:
		main()