r1 = open("vertices1.csv", "r")
r2 = open("vertices.csv", "r")
f = open("userRatings.csv", "w")

content = r1.readlines()

for line in content:
	ids = line.split("\"[")[0]
	mems = line.split("\"[")[1]
	mems = mems[:-2]
	mems = mems.split()
	for i in range(0, 1000):
		if str(i) in mems:
			f.write(ids + str(i) + ",1\n")
		else:
			f.write(ids + str(i) + ",0\n")

content = r2.readlines()

for line in content:
	ids = line.split("\"[")[0]
	mems = line.split("\"[")[1]
	mems = mems[:-2]
	mems = mems.split()
	for i in range(0, 200):
		if str(i) in mems:
			f.write(ids + str(i) + ",1\n")
		else:
			f.write(ids + str(i) + ",0\n")

r1.close()
r2.close()
f.close()