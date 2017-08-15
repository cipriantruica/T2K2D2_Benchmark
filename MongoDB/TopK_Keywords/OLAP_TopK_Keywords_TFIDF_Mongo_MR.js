// using map-reduce - slower than aggregation functions
TFIDF_MR_TopKWords = function(q, k){
        // compute the number of times the word apear in the corpus and the TF sum
        mapFunction = function() {
                        for (var idx=0; idx<this.words.length; idx++){
                            emit(this.words[idx].word, { count: 1, stf: this.words[idx].tf });
                        }
                    }
        reduceFunction = function(key, values) {
                            reducedVal = { count: 0, stf: 0 };
                            for (var idx = 0; idx < values.length; idx++) {
                                reducedVal.count += values[idx].count;
                                reducedVal.stf += values[idx].stf;
                            }
                            return reducedVal;
                        };
        // The function  that computes the TFIDF
        finalizeFunction = function(key, reducedVal){
            reducedVal.tfidf = reducedVal.stf * (1 + Math.log(noDocs/reducedVal.count));
            return reducedVal;
        }
        var noDocs = db.document_facts.count(q);
        db.document_facts.mapReduce(mapFunction, reduceFunction, {query: q, out: "vocabulary", scope: {noDocs: noDocs}, finalize: finalizeFunction});
        // print the values
        var items = db.vocabulary.find().sort({"value.tfidf": -1}).limit(k).addOption(DBQuery.Option.noTimeout);
        return items;
    }


