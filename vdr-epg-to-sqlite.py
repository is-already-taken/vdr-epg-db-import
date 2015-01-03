# -*- coding: utf-8 -*-

import re
import sqlite3

FILE = "/var/cache/vdr/epg.data"
DB = "/ram/epg.sqlite3"

# create table channels(name varchar(64), sat_name varchar(16), id1 integer, id2 integer, id3 integer, unique(sat_name, id1, id2, id3));
# create table programs(channel_id integer, title varchar(256), subtitle varchar(256), description varchar(2048), start_time integer, duration integer, unique(channel_id, start_time));

# C S19.2E-1-1089-12060 VOX
# E 21132 1420236600 3000 4E 11
# T Law & Order: Special Victims Unit
# D 207/X Retrovirus. Ein fünf Monate alter AIDS-kranker Säugling, der nicht gegen HIV behandelt wurde, bringt die Sondereinheit auf die Spur des Arztes Dr. Hutton, der konsequent leugnet, dass das HI-Virus AIDS verursacht. Der Mediziner ist davon überzeugt, dass AIDS eine weltweite Konspiration der Pharmakonzerne ist, um mit der Angst der Bevölkerung ein Milliardengeschäft zu machen. So behandelt er seine HIV-infizierten Patienten mit Vitaminen und Joghurt, oder mit Antibiotika...
# G 10
# e
# E 21125 1420239600 1200 4E 11
# T vox nachrichten
# e

# E 30131 1420351800 1500 50 7
# E XXXXX TTTTTTTTTT DDDD XX X     T=timestamp   D=duration
event_matcher = re.compile("^[0-9]+ ([0-9]+) ([0-9]+) [A-F0-9]+ [A-F0-9]+$")
channel_matcher = re.compile("^([^ ]+)-([0-9]+)-([0-9]+)-([0-9]+) (.+)$")


conn = sqlite3.connect(DB)

curs = conn.cursor()

curs.execute("DELETE FROM channels")
curs.execute("DELETE FROM programs")
conn.commit()

with open(FILE, "r") as f:
	line_no = -1

	# 1st initialization
	subtitle = None

	for line in f:
		line_no += 1

		line_type = line[0]

		# ignore these lines
		if line_type in ["G", "d", "V", "X"]:
			# G - unknown, uninteresting (so far)
			# X - additional audio info?
			# d - 2nd description?
			# V - VPS time?
			continue

		line_rest = line[2:].strip("\n")

		if line_type == "C":
			channel_match = channel_matcher.match(line_rest)

			if not channel_match:
				print "Error reading \"C\" line \"%s\"" % line_rest
				break

			sat_name = channel_match.group(1)
			id1 = channel_match.group(2)
			id2 = channel_match.group(3)
			id3 = channel_match.group(4)
			channel_name = channel_match.group(5)

			placeholders = (buffer(channel_name), sat_name, id1, id2, id3)

			try:
				curs.execute("INSERT INTO channels (name, sat_name, id1, id2, id3) VALUES(?, ?, ?, ?, ?)", placeholders)
				channel_id = curs.lastrowid
			except Exception as e:
				print "SQL error with (%s, %s, %s, %s, %s)" % placeholders
				raise e

			continue

		if line_type == "T":
			title = line_rest
			continue

		if line_type == "D":
			description = line_rest
			continue

		if line_type == "S":
			subtitle = line_rest
			continue

		if line_type == "E":
			parts = line_rest.split(" ")

			if len(parts) != 5:
				print "Error reading \"E\" line. Expected 5 fields, got %d" % (len(parts))
				break

			start_time = int(parts[1])
			duration = int(parts[2])

			continue

		if line_type == "e":
			if description == None:
				description = ""

			if subtitle == None:
				subtitle = ""

			descr_ = description[0:20] if len(description) > 20 else description
			print "%6d: %s: %s (%s ...) (@%d; %d)" % (line_no, channel_name, title, descr_, start_time, duration)

			curs.execute("INSERT INTO programs (channel_id, title, subtitle, description, start_time, duration) VALUES(?, ?, ?, ?, ?, ?)", (channel_id, buffer(title), buffer(subtitle), buffer(description), start_time, duration))

			description = None
			title = None
			subtitle = None
			start_time = None
			duration = None


conn.commit()
conn.close()
