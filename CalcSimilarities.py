import graphlab as gl

epsilon = 1

def calcSim(src, edge, dst):
	if src['first'] == None:
		if dst['first'] == None:
			edge['weight'] = epsilon
		else:
			edge['weight'] = len(dst['first']) + epsilon
	else:
		if dst['first'] == None:
			edge['weight'] = len(src['first']) + epsilon
		else:
			edge['weight'] = len(src['first']) + len(dst['first']) - 2 * (len(set(src['first']).intersection(dst['first']))) + epsilon
	return (src, edge, dst)


if __name__ == "__main__":
	graph = gl.load_sgraph("s3://sdurgam/GraphLab/Graph")
	graph.edges['weight'] = 0.0
	graph = graph.triple_apply(calcSim, mutated_fields = ['weight'])
	variance = graph.edges['weight'].var()
	graph.edges['weight'] = graph.edges['weight'].apply(lambda x: x / variance)
	graph.vertices.remove_column('first')
	graph.save("s3://sdurgam/GraphLab/Graph")