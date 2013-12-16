// Sample

// Utility
var util = {
        // innerproduct
        innerproduct : function (v1, v2) {
                var sum = 0;
                for(var key in v1) {
                        if (v2[key]) {
                                sum += v1[key] * v2[key];
                        }
                }
                return sum;
        },
        // norm
        norm : function (v) {
                var sum = 0;
                for(var key in v) {
                        sum += v[key] * v[key];
                }
                return Math.sqrt(sum);
        }
}

var tweets_search = function (keyword) {
	db.tweets_tfidf.mapReduce(
		function () {
			var s = util.innerproduct(this.value.v, keyword) / this.value.l
			if (0 < s) emit(this._id, s);
		},
		function(key,values) {
			return values[0];
		},
		{scope: {util: util, keyword: keyword}, out: 'tweets_search_result'}
	);
	db.tweets_search_result.find().sort({value:-1}).limit(5).forEach(function (r) {
		var result = db.tweets.findOne({_id:r._id},{content:1});
		if (result) {
			similar = [];
			db.tweets_similarity.find({a:result._id},{b:1, score:1}).sort({score:-1}).limit(3).forEach(function (sr) {
				var similar_result = db.tweets.findOne({_id:sr.b},{content:1});
				if (similar_result)
					similar.push({content: similar_result.content, score: sr.score});
			});
			printjson({content: result.content, score: r.value, similar: similar});
		}
	});
}

tweets_search({"job":1, "today":1});


