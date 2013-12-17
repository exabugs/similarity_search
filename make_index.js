//
//
//

var master_name = 'tweets';

// Utility
var util = {
	// product
	product : function (a, v) {
		for (var i = 0; i < a.length; i++) {
			if (v[a[i].k])
				a[i].w = a[i].v * v[a[i].k];
			else
				a[i].w = a[i].v;
		}
		return a;
	},
	// norm
	norm : function (a) {
		var sum = 0;
		for (var i = 0; i < a.length; i++) {
			if (a[i].w) {
				sum += a[i].w * a[i].w;
			}
		}
		return Math.sqrt(sum);
	},
	// innerproduct
	innerproduct : function (a, v) {
		var sum = 0;
		for (var i = 0; i < a.length; i++) {
			var info = a[i];
			if (v[info.k]) {
				sum += info.w * v[info.k];
			}
		}
		return sum;
	},
	// to_hash
	to_hash : function (a) {
		var ret = {};
		for (var i = 0; i < a.length; i++) {
			if (a[i].w) {
				ret[a[i].k] = a[i].w;
			}
		}
		return ret;
	}
}

// IDF Dictionary
db[master_name].mapReduce(
	function () {
		for (var i = 0; i < this.tf.v.length; i++)
			emit(this.tf.v[i].k, 1);
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
		var v = util.product(this.tf.v, dic);
		emit(this._id, {v: v, l: util.norm(v)});
	},
	function(key,values) {
		return values[0];
	},
	{scope: {util: util, dic: dic}, out: master_name+'_tfidf'}
);
db[master_name+'_tfidf'].find().forEach(function (tgt) {
	db[master_name].update({_id: tgt._id},{$set: {tf: tgt.value}});
});
db[master_name+'_tfidf'].drop();

// Cosine Similarity
db[master_name+'_similarity'].drop();
var cursor = db[master_name].find();
while (cursor.hasNext()) {
	var src = cursor.next();
	src.tf.condition = util.to_hash(src.tf.v);
	db[master_name].mapReduce(
		function () {
			var s = util.innerproduct(this.tf.v, src.tf.condition);
			if (0 < s) emit(this._id, s / this.tf.l / src.tf.l);
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
db[master_name+'_tmp'].drop();


