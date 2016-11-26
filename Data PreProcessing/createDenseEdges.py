file = open("Edges.txt", "w")

file.write("src")
file.write("\t")
file.write("dst")
file.write("\n")

for i in range(0, 100):
	for j in range(0, 100):
		file.write(i)
		file.write("\t")
		file.write(j)
		file.write("\n")