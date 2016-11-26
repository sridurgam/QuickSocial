from random import randint

file = open("Edges.txt", "w")

file.write("src\tdst\n")

d = []

r1 = randint(0, 99)
r2 = randint(0, 99)

for i in range(0, 1500):
	if [r1, r2] not in d:
		d.append([r1, r2])
		d.append([r2, r1])
		file.write(r1)
		file.write("\t")
		file.write(r2)
		file.write("\n")
		
	r1 = randint(0, 99)
	r2 = randint(0, 99)