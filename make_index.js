
var master_name = 'tweets';

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
	},
	// product
	product : function (v1, v2) {
		var v = {};
		for(var key in v1) {
			if (v2[key]) {
				v[key] = v1[key] * v2[key];
			}
		}
		return v;
	}
}

// IDF Dictionary
db[master_name].mapReduce(
	function () {
		for (key in this.tf) {emit(key, 1);};
	},
	function(key,values) {
		return Array.sum(values);
	},
	{finalize: function(key, value) {return Math.log(N/value);}, scope: {N: db[master_name].count()}, out: master_name+'_idf'}
);

// TF-IDF
var dic = {};
db[master_name+'_idf'].find().sort({value:-1}).limit(100000).forEach( function (idf) {dic[idf._id] = idf.value} );
db[master_name].mapReduce(
	function () {
		var v = util.product(this.tf, dic);
		emit(this._id, {v: v, l: util.norm(v)});
	},
	function(key,values) {
		return values[0];
	},
	{scope: {util: util, dic: dic}, out: master_name+'_tfidf'}
);

// Cosine Similarity
db[master_name+'_similarity'].drop();
var cursor = db[master_name+'_tfidf'].find();
while (cursor.hasNext()) {
	var src = cursor.next();
	db[master_name+'_tfidf'].mapReduce(
		function () {
			var s = util.innerproduct(this.value.v, src.value.v);
			if (0 < s) emit(this._id, s / this.value.l / src.value.l);
		},
		function(key,values) {
			return values[0];
		},
		{scope: {util: util, src: src}, query: {_id:{$gt:src._id}}, out: master_name+'_tmp'}
	);
	db[master_name+'_tmp'].find().forEach(
		function (dst) {
			db[master_name+'_similarity'].insert({a:dst._id, b:src._id, score:dst.value});
			db[master_name+'_similarity'].insert({a:src._id, b:dst._id, score:dst.value});
		}
	);
}
