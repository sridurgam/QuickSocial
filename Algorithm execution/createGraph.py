import graphlab as gl

gl.aws.set_credentials('AKIAIBR7N2DK6M3WI5XQ','W7wcnq77f6rY3aBULN/hYmkg0+0KVs3prw7/s42k')

vertices = gl.SFrame("s3://sdurgam/Spark/Vertices/UtoC.txt")

vertices['firstVector'] = vertices['ids'].apply(lambda x: x.split()[1:])
vertices['memVector'] = vertices['ids'].apply(lambda x: x.split()[1:])
vertices['ids'] = vertices['ids'].apply(lambda x: x.split()[0])

vertices['isSuperNode'] = vertices['firstVector'].apply(lambda x: len(x))
vertices = vertices.sort('isSuperNode', ascending = False)
vertices['isSuperNode'] = vertices['isSuperNode'].apply(lambda x: 1 if x > 65 else 0)

def firstFn(array):
	vector = []
	for a in array:
		if int(a) % 97000 < 5000:
			vector.append(int(a) % 97000)
	return vector

def lastFn(array):
	vector = []
	for a in array:
		if int(a) % 97000 >= 5000:
			vector.append(int(a) % 97000)
	return vector

vertices['firstVector'] = vertices['firstVector'].apply(lambda x: firstFn(x))
vertices['memVector'] = vertices['memVector'].apply(lambda x: lastFn(x))
vertices.save("s3://sdurgam/GraphLab/Vertices")

edges = gl.SFrame.read_csv("s3://sdurgam/Spark/Edges/ungraph*", delimiter = "\t", column_type_hints=[int, int])
edges.save("s3://sdurgam/GraphLab/Edges")

graph = gl.SGraph()
graph = graph.add_vertices(vertices, vid_field = 'ids')
graph = graph.add_edges(edges, src_field = 'src', dst_field = 'dst')

graph.save("s3://sdurgam/GraphLab/Graph")