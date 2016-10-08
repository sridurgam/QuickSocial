import graphlab as gl
from bitarray import bitarray

NUM_FIRST = 5000
NUM_NEXT = 1615991

def firstFn(x):
	r = NUM_FIRST * bitarray('0')
	for item in x:
		i = int(item)
		if i < NUM_FIRST:
			r[i] = True
	return r

def nextFn(x):
	r = NUM_NEXT * bitarray('0')
	for item in x:
		i = int(item)
		if i > NUM_FIRST:
			r[i - NUM_FIRST] = True
	return r


sf = gl.SFrame('output/UtoC.txt')

sf['first'] = sf['ids'].apply(lambda x: x.split()[1:])
sf['next'] = sf['first'].apply(lambda x: nextFn(x))
sf['first'] = sf ['first'].apply(lambda x: firstFn(x))
sf['ids'] = sf['ids'].apply(lambda x: int(x.split()[0]))


edges = gl.SFrame.read_csv("../Friendster Dataset/SNAP/Graph.txt", skiprows = 3, delimiter='	')

graph = gl.SGraph()

graph = graph.add_vertices(sf, vid_field = 'ids')
graph = graph.add_edges(edges, src_field='# FromNodeId', dst_field='ToNodeId')

graph.save("Graph")