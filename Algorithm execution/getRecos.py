NUM_USERS = 7944949
NUM_FIRST = 5000
NUM_SUPERNODES = 160000

def getRecos(array):
	vector = []
	itr = 0
	for i in range(0, 10):
		vector.append(array.index(max(array)) + NUM_FIRST + itr)
		array.remove(max(array))
		itr += 1
	return vector

def getNumRecos(src, edge, dst):
	src['rightRecos'] = len(set(src['recos']).intersection(src['groundTruth']))
	dst['rightRecos'] = len(set(dst['recos']).intersection(dst['groundTruth']))
	return (src, edge, dst)

if __name__ == "__main__":
	graph = load_sgragh("s3://sank/GraphLab/Graph")
	graph.vertices['recos'] = graph.vertices['prev'].apply(lambda x: getRecos(x))

	graph.vertices['groundTruth'] = groundTruth
	graph.vertices['rightRecos'] = 0
	graph = graph.triple_apply(getNumRecos, mutated_fields=['rightRecos'])

	r1 = list(graph.vertices.sort('__id')['rightRecos'][NUM_SUPERNODES:]).count(1)
	r2 = list(graph.vertices.sort('__id')['rightRecos'][NUM_SUPERNODES:]).count(2)
	r3 = list(graph.vertices.sort('__id')['rightRecos'][NUM_SUPERNODES:]).count(3)
	r4 = list(graph.vertices.sort('__id')['rightRecos'][NUM_SUPERNODES:]).count(4)
	r5 = list(graph.vertices.sort('__id')['rightRecos'][NUM_SUPERNODES:]).count(5)

	c1 = r1 + r2 + r3 + r4 + r5
	c2 = r2 + r3 + r4 + r5
	c3 = r3 + r4 + r5
	c4 = r4 + r5

	print("The percentage of users who had at least one right recommendation: " + str(c1))
	print("The percentage of users who had at least two right recommendation: " + str(c2))
	print("The percentage of users who had at least three right recommendation: " + str(c3))
	print("The percentage of users who had at least four right recommendation: " + str(c4))

	graph.save("s3://sank/GraphLab/Graph")
