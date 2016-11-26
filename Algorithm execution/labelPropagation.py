from numpy import array
import numpy
import graphlab as gl

CONVERGENCE_VALUE = 0.1
NUM_NON_SUPERNODES = int(0.8 * 7944949)

def propagate(src, edge, dst):
	src['memVector'] += array([(edge['weight']/src['sumWeights']) * x for x in dst['prev']])
	dst['memVector'] += array([(edge['weight']/dst['sumWeights']) * x for x in src['prev']])
	return (src, edge, dst)

def initialise(src,edge,dst):
	dst['prev'] = array(dst['prev'])*dst['isSuperNode']
	src['prev'] = array(src['prev'])*src['isSuperNode']
	return (src,edge,dst)

def l2Norm(src,edge,dst):
	if numpy.linalg.norm(numpy.array(src['memVector']),ord=2) > 0.0:
		if numpy.linalg.norm(numpy.array(src['prev'])-numpy.array(src['memVector']),ord = 2) < CONVERGENCE_VALUE:
			src['isSuperNode'] = 1
	if numpy.linalg.norm(numpy.array(dst['memVector']),ord=2) > 0.0:
		if numpy.linalg.norm(numpy.array(dst['prev'])-numpy.array(dst['memVector']),ord = 2) < CONVERGENCE_VALUE:
			dst['isSuperNode'] = 1
	return (src, edge, dst)

def updatePrev(src, edge, dst):
	if src['isSuperNode'] == 0:
		src['prev'] = src['memVector']
	if dst['isSuperNode'] == 0:
		dst['prev'] = dst['memVector']
	return (src, edge, dst)

if __name__ == '__main__':
	graph = gl.load_sgraph("s3://sdurgam/GraphLab/Graph")
	graph = graph.triple_apply(initialise,mutated_fields=['prev'])

	convergence = graph.vertices['isSuperNode'].sum()

	while (convergence < NUM_NON_SUPERNODES):
		graph = graph.triple_apply(propagate, mutated_fields=['memVector'])
		graph = graph.triple_apply(l2Norm, mutated_fields=['isSuperNode'])
		graph = graph.triple_apply(updatePrev, mutated_fields=['prev'])
		graph.vertices['memVector'] = graph.vertices['memVector'].apply(lambda x: [0.0] * 92000)
		convergence = graph.vertices['isSuperNode'].sum()

	graph = graph.save("s3://sdurgam/GraphLab/Graph")