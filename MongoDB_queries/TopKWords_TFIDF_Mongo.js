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
        var noDocs = db.documents.count(q);
        db.documents.mapReduce(mapFunction, reduceFunction, {query: q, out: "vocabulary", scope: {noDocs: noDocs}, finalize: finalizeFunction});
        // print the values
        var items = db.vocabulary.find().sort({"value.tfidf": -1}).limit(k).addOption(DBQuery.Option.noTimeout);
        while(items.hasNext()){
            var item = items.next();
            print(item._id + ' ' + item.value.tfidf);
        }
    }

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
        while(items.hasNext()){
            var item = items.next();
            print(item.word + ' ' + item.TFIDF);
        }
    }

//testing function
TFIDF_PA_testingFunction = function(){
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
        TFIDF_PA_TopKWords(q1a, 10);
        endTime = new Date();
        times['male']['q1'].push(endTime - startTime);
        db.vocabulary.drop();
        print('Q1 - female');
        startTime = new Date();
        TFIDF_PA_TopKWords(q1b, 10);        
        endTime = new Date();
        times['female']['q1'].push(endTime - startTime);
        db.vocabulary.drop();
        print('Q2 - male');
        startTime = new Date();
        TFIDF_PA_TopKWords(q2a, 10);
        endTime = new Date();
        times['male']['q2'].push(endTime - startTime);
        db.vocabulary.drop();
        print('Q2 - female');
        startTime = new Date();
        TFIDF_PA_TopKWords(q2b, 10);
        endTime = new Date();
        times['female']['q2'].push(endTime - startTime);
        db.vocabulary.drop();
        print('Q3 - male');
        startTime = new Date();
        TFIDF_PA_TopKWords(q3a, 10);
        endTime = new Date();
        times['male']['q3'].push(endTime - startTime);
        db.vocabulary.drop();
        print('Q3 - female');
        startTime = new Date();
        TFIDF_PA_TopKWords(q3b, 10);
        endTime = new Date();
        times['female']['q3'].push(endTime - startTime);
        db.vocabulary.drop();
        print('Q4 - male');
        startTime = new Date();
        TFIDF_PA_TopKWords(q4a, 10);
        endTime = new Date();
        times['male']['q4'].push(endTime - startTime);
        db.vocabulary.drop();
        print('Q4 - female');
        startTime = new Date();
        TFIDF_PA_TopKWords(q4b, 10);
        endTime = new Date();
        times['female']['q4'].push(endTime - startTime);
        db.vocabulary.drop();
    }
    print('TFIDF PA');
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

//testing function
TFIDF_MR_testingFunction = function(){
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
        TFIDF_MR_TopKWords(q1a, 10);
        endTime = new Date();
        times['male']['q1'].push(endTime - startTime);
        db.vocabulary.drop();
        print('Q1 - female');
        startTime = new Date();
        TFIDF_MR_TopKWords(q1b, 10);        
        endTime = new Date();
        times['female']['q1'].push(endTime - startTime);
        db.vocabulary.drop();
        print('Q2 - male');
        startTime = new Date();
        TFIDF_MR_TopKWords(q2a, 10);
        endTime = new Date();
        times['male']['q2'].push(endTime - startTime);
        db.vocabulary.drop();
        print('Q2 - female');
        startTime = new Date();
        TFIDF_MR_TopKWords(q2b, 10);
        endTime = new Date();
        times['female']['q2'].push(endTime - startTime);
        db.vocabulary.drop();
        print('Q3 - male');
        startTime = new Date();
        TFIDF_MR_TopKWords(q3a, 10);
        endTime = new Date();
        times['male']['q3'].push(endTime - startTime);
        db.vocabulary.drop();
        print('Q3 - female');
        startTime = new Date();
        TFIDF_MR_TopKWords(q3b, 10);
        endTime = new Date();
        times['female']['q3'].push(endTime - startTime);
        db.vocabulary.drop();
        print('Q4 - male');
        startTime = new Date();
        TFIDF_MR_TopKWords(q4a, 10);
        endTime = new Date();
        times['male']['q4'].push(endTime - startTime);
        db.vocabulary.drop();
        print('Q4 - female');
        startTime = new Date();
        TFIDF_MR_TopKWords(q4b, 10);
        endTime = new Date();
        times['female']['q4'].push(endTime - startTime);
        db.vocabulary.drop();
    }
    print('TFIDF MR');
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

