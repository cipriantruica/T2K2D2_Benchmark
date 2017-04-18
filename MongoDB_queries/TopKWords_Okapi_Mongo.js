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
       
        // print the values
        var items = db.vocabulary.find().sort({"value.sokapi": -1}).limit(top).addOption(DBQuery.Option.noTimeout);
        while(items.hasNext()){
            var item = items.next();
            print(item._id + ' ' + item.value.sokapi);
        }
    }

Okapi_testingFunction = function(){

    startDate = new Date('2015-09-17T00:00:00Z');
    endDate = new Date('2015-09-18T00:00:00Z');

    q1a = {gender:   'male'};
    q1b = {gender: 'female'};
    q2a = {gender:   'male', date: {$gte: startDate, $lte: endDate}};
    q2b = {gender: 'female', date: {$gte: startDate, $lte: endDate}};
    q3a = {gender:   'male', "geoLocation.0": {$gte: 20, $lte: 40}, "geoLocation.1": {$gte: -100, $lte: 100}};
    q3b = {gender: 'female', "geoLocation.0": {$gte: 20, $lte: 40}, "geoLocation.1": {$gte: -100, $lte: 100}};
    q4a = {gender:   'male', date: {$gte: startDate, $lte: endDate}, "geoLocation.0": {$gte: 20, $lte: 40}, "geoLocation.1": {$gte: -100, $lte: 100}};
    q4b = {gender: 'female', date: {$gte: startDate, $lte: endDate}, "geoLocation.0": {$gte: 20, $lte: 40}, "geoLocation.1": {$gte: -100, $lte: 100}};

    var k1 = 1.6;
    var b = 0.75
    var top = 10;

    var times = {
        'male' : {
            'q1': new Array(),
            'q2': new Array(),
            'q3': new Array(),
            'q4': new Array()
        },
        'female' : 
        {
            'q1': new Array(),
            'q2': new Array(),
            'q3': new Array(),
            'q4': new Array()
        }
    };

    db.vocabulary.drop();

    for (i=0; i<40; i++){
        print(i+1);
        print('Q1 - male');
        startTime = new Date();
        Okapi_TopKWords(q1a, k1, b, top);
        endTime = new Date();
        times['male']['q1'].push(endTime - startTime);
        db.vocabulary.drop();

        print('Q1 - female');
        startTime = new Date();
        Okapi_TopKWords(q1b, k1, b, top);
        endTime = new Date();
        times['female']['q1'].push(endTime - startTime);
        db.vocabulary.drop();

        print('Q2 - male');
        startTime = new Date();
        Okapi_TopKWords(q2a, k1, b, top);
        endTime = new Date();
        times['male']['q2'].push(endTime - startTime);
        db.vocabulary.drop();

        print('Q2 - female');
        startTime = new Date();
        Okapi_TopKWords(q2b, k1, b, top);
        endTime = new Date();
        times['female']['q2'].push(endTime - startTime);
        db.vocabulary.drop();

        print('Q3 - male');
        startTime = new Date();
        Okapi_TopKWords(q3a, k1, b, top);
        endTime = new Date();
        times['male']['q3'].push(endTime - startTime);
        db.vocabulary.drop();

        print('Q3 - female');
        startTime = new Date();
        Okapi_TopKWords(q3b, k1, b, top);
        endTime = new Date();
        times['female']['q3'].push(endTime - startTime);
        db.vocabulary.drop();

        print('Q4 - male');
        startTime = new Date();
        Okapi_TopKWords(q4a, k1, b, top);
        endTime = new Date();
        times['male']['q4'].push(endTime - startTime);
        db.vocabulary.drop();
    
        print('Q4 - female');
        startTime = new Date();
        Okapi_TopKWords(q4b, k1, b, top);
        endTime = new Date();
        times['female']['q4'].push(endTime - startTime);
        db.vocabulary.drop();
    }
    print('Okapi MR');
    print('Q1 - male');
    print(times['male']['q1']);

    print('Q1 - female');
    print(times['female']['q1']);

    print('Q2 - male');
    print(times['male']['q2']);

    print('Q2 - female');
    print(times['female']['q2']);

    print('Q3 - male');
    print(times['male']['q3']);

    print('Q3 - female');
    print(times['female']['q3']);

    print('Q4 - male');
    print(times['male']['q4']);

    print('Q4 - female');
    print(times['female']['q4']);
}
