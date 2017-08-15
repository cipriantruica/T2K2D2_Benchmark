// using map-reduce - slower than aggregation functions
Okapi_TopKWords = function(q, k1, b, top){
        // compute the number of times the word apear in the corpus and the TF sum
        mapFunction = function() {
                        for (var idx=0; idx<this.words.length; idx++){
                            emit(this.words[idx].word, { count: 1, stf: [{tf: this.words[idx].tf, docLen: this.lemmaTextLength}] });
                        }
                    }

        reduceFunction = function(key, values) {
                            reducedVal = { count: 0, stf: [] };
                            for (var idx = 0; idx < values.length; idx++) {
                                reducedVal.count += values[idx].count;
                                reducedVal.stf = reducedVal.stf.concat(values[idx].stf);
                            }
                            return reducedVal;
                        };
        
        finalizeFunction = function(key, reducedVal){
            print(reducedVal);
            var stfidf = 0;
            var sokapi = 0;
            for (var idx = 0; idx < reducedVal.stf.length; idx++){
                reducedVal.stf[idx].tfidf = reducedVal.stf[idx].tf * (1 + Math.log(noDocs/reducedVal.count));
                stfidf += reducedVal.stf[idx].tf * (1 + Math.log(noDocs/reducedVal.count));
                reducedVal.stf[idx].okapi = (reducedVal.stf[idx].tf * (1 + Math.log(noDocs/reducedVal.count)) * (k1 + 1))/(reducedVal.stf[idx].tf + k1 * (1 - b + b *  reducedVal.stf[idx].docLen/avgDL))
                sokapi += (reducedVal.stf[idx].tf * (1 + Math.log(noDocs/reducedVal.count)) * (k1 + 1))/(reducedVal.stf[idx].tf + k1 * (1 - b + b *  reducedVal.stf[idx].docLen/avgDL));
            }
            reducedVal.stfidf = stfidf;
            reducedVal.sokapi = sokapi;
            return reducedVal;
        }

        var noDocs = db.documents.count(q);
        var avgDL;
        
        db.documents.aggregate([ { $match: q}, { $group: { _id: null, avgDL: { $avg: "$lemmaTextLength" }}}]).forEach(function(elem){avgDL = elem.avgDL;})

        db.documents.mapReduce(mapFunction, reduceFunction, {query: q, out: "vocabulary", scope: {noDocs: noDocs, k1: k1, b: b, avgDL: avgDL}, finalize: finalizeFunction});
       
        var items = db.vocabulary.find().sort({"value.sokapi": -1}).limit(top).addOption(DBQuery.Option.noTimeout);
        return items;
    }

