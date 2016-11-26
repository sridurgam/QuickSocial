import graphlab
import time

t0 = time.time()
edge_data = graphlab.SFrame.read_csv('/Users/sank/Desktop/CloudComputing/presentation/pagerank/pagerank_edges')
g = graphlab.SGraph()
g = g.add_edges(edge_data, src_field='__src_id', dst_field='__dst_id')
print g.summary()

pr = graphlab.pagerank.create(g,max_iterations=100,threshold=0.0001)
t1 = time.time()
print pr.summary()
pagerank_output = pr['pagerank']
print pagerank_output.topk('pagerank', k=10)
print t1 - t0
