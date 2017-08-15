// using pipeline aggregation - faster than MapReduce
TFIDF_PA_TopKWords = function(q, k){
        var docs = Array();
        noDocs = db.documents.count(q);
        // aggregate function that computs the number of times the word appears in the coprus and the TF sum
        db.documents.aggregate([
            { $match: q },
            { $unwind: "$words" },
            { $project: { word: '$words.word', wtf: '$words.tf' } },
            { $group: { _id: "$word", sum_tf: { $sum: "$wtf" }, count: { $sum: 1 } } }
        ]).forEach(function(elem){
            var tfidf = elem.sum_tf * (1 + Math.log(noDocs/elem.count));
            var doc = {word: elem._id, TFIDF: tfidf};
            docs.push(doc);
        });
        db.vocabulary.insert(docs);
        var items = db.vocabulary.find().sort({TFIDF: -1}).limit(k).addOption(DBQuery.Option.noTimeout);
        return items;
    }

