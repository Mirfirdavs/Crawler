import sqlite3


sqlFullQuery = """select
	w0.fk_URLId  fk_URLId
	, w0.location w0_loc
	, w1.location w1_loc
	from wordLocation w0
	inner join wordLocation w1
	on w0.fk_URLId = w1.fk_URLId
	where
	w0.fk_wordId = 1
	and w1.fk_wordId = 2
"""

conn = sqlite3.connect('dbfilename.db')
cur = conn.execute(sqlFullQuery).fetchall()
print(cur)


#select
#w0.fk_URLId  fk_URLId
#, w0.location w0_loc
#, w1.location w1_loc
#from wordLocation w0
#inner join wordLocation w1
#on w0.fk_URLId = w1.fk_URLId
#where
#w0.fk_wordId = 1
#and w1.fk_wordId = 2
#
#
#SELECT fk_URLId, w0.location, w1.location
#	FROM wordLocation 
#	w0 inner join wordLocation w1 on w0.fk_URLId
#	WHERE w0.fk_URLId = 4 AND w1.fk_URLId = 91#