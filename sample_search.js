// 検索機能 サンプル
var tweets_search = function (input) {
	var keyword = [];
	for (var i = 0; i < input.length; i++)
		keyword.push({k:input[i], w:1});
	var condition = util.to_hash(keyword);
printjson(condition);
	// 検索の実行
	db[master_name].mapReduce(
		function () {
			var s = util.innerproduct(this.tf.v, condition) / this.tf.l
			if (0 < s) emit(this._id, s);
		},
		function(key,values) {
			return values[0];
		},
		{query: {"tf.v.k": {$in: input}}, scope: {util: util, condition: condition}, out: 'tweets_search_result'}
	);
	// 検索結果の表示
	db.tweets_search_result.find().sort({value:-1}).limit(5).forEach(function (r) {
		var result = db[master_name].findOne({_id:r._id},{content:1});
		if (result) {
			// 検索結果に類似したtweetを見つける
			similar = [];
			db.tweets_similarity.find({a:result._id},{b:1, score:1}).sort({score:-1}).limit(3).forEach(function (sr) {
				var similar_result = db.tweets.findOne({_id:sr.b},{content:1});
				if (similar_result)
					similar.push({content: similar_result.content, score: sr.score});
			});
			// 検索結果の表示
			printjson({content: result.content, score: r.value, similar: similar});
		}
	});
}


tweets_search(["説明"]);
tweets_search(["会社","正月"]);

