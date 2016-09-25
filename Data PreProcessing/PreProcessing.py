import graphlab as gl

NUM_COMMUNITIES = 1620991

sf = gl.SFrame('../Friendster Dataset/SNAP/Communities.txt')

array = []

for i in range(0, NUM_COMMUNITIES):
	array.append(len(sf[i]['Communities'].split()))

sA = gl.SArray(array)

sf.add_column(sA, "Size")
sf = sf.sort("Size", False)

f = open('firstCommunities.txt', 'w')
for i in range(0, 5000):
	f.write(str(i) + "\t");
	f.write(sf[i]['Communities'])
	f.write("\n")
f.close()

f = open('NextCommunities.txt', 'w')
for i in range(5000, NUM_COMMUNITIES):
	f.write(str(i) + "\t")
	f.write(sf[i]['Communities'])
	f.write("\n")

f.close()