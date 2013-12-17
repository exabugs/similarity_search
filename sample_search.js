// $B8!:w5!G=(B $B%5%s%W%k(B
var tweets_search = function (keyword) {
	var condition = util.to_hash(keyword);
printjson(condition);
	// $B8!:w$N<B9T(B
	db[master_name].mapReduce(
		function () {
			var s = util.innerproduct(this.tf.v, condition) / this.tf.l
			if (0 < s) emit(this._id, s);
		},
		function(key,values) {
			return values[0];
		},
		{scope: {util: util, condition: condition}, out: 'tweets_search_result'}
	);
	// $B8!:w7k2L$NI=<((B
	db.tweets_search_result.find().sort({value:-1}).limit(5).forEach(function (r) {
		var result = db[master_name].findOne({_id:r._id},{content:1});
		if (result) {
			// $B8!:w7k2L$KN`;w$7$?(Btweet$B$r8+$D$1$k(B
			similar = [];
			db.tweets_similarity.find({a:result._id},{b:1, score:1}).sort({score:-1}).limit(3).forEach(function (sr) {
				var similar_result = db.tweets.findOne({_id:sr.b},{content:1});
				if (similar_result)
					similar.push({content: similar_result.content, score: sr.score});
			});
			// $B8!:w7k2L$NI=<((B
			printjson({content: result.content, score: r.value, similar: similar});
		}
	});
}

tweets_search([{k:"$B2q<R(B",w:1}]);
tweets_search([{k:"$B$*Ck(B",w:1}]);
tweets_search([{k:"$B$*@57n(B",w:1}]);
