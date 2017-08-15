// using pipeline aggregation - faster than MapReduce
TFIDF_PA_TopKWords = function(q, k){
        var docs = Array();
        noDocs = db.document_facts.count(q);
        // aggregate function that computs the number of times the word appears in the coprus and the TF sum
        db.document_facts.aggregate([
            { $match: q },
            { $unwind: "$words" }, 
            { $group: { _id: "$words.word", count: { $sum: 1 }, sum_tf: { $sum: "$words.tf" } } },
        ]).forEach(function(elem){
            var tfidf = elem.sum_tf * (1 + Math.log(noDocs/elem.count));
            var doc = {word: elem._id, TFIDF: tfidf};
            docs.push(doc);
        });
        db.vocabulary.insert(docs);
        var items = db.vocabulary.find().sort({TFIDF: -1}).limit(k).addOption(DBQuery.Option.noTimeout);
        return items;
    }
