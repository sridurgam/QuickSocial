'''
GraphLab's inbuilt stochastic gradient descent based recommender
'''

import graphlab as gl
import gl.aggregate as agg

def recommend()
	file = gl.SFrame.read_csv('s3://sdurgam/inputfiles/userRatings.csv')
	model = gl.recommender.create(file, target='ratings')
	results = model.recommend()
	results_stats = results.groupby("user_id",{"memDict":agg.CONCAT("item_id")})
	result_stats
	gl.results_stats.save("s3://sdurgam/outputfiles/results.csv",format='csv')

if __name__ == '__main__':
	recommend()