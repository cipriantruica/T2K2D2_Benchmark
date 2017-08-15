Okapi_testingFunction = function(){

    startDate = new Date('2015-09-17T00:00:00Z');
    endDate = new Date('2015-09-18T00:00:00Z');

    q1a = {"author_dimension.gender":   'male'};
    q1b = {"author_dimension.gender": 'female'};
    q2a = {"author_dimension.gender":   'male', "time_dimension.full_date": {$gte: startDate, $lte: endDate}};
    q2b = {"author_dimension.gender": 'female', "time_dimension.full_date": {$gte: startDate, $lte: endDate}};
    q3a = {"author_dimension.gender":   'male', "location_dimension.X": {$gte: 20, $lte: 40}, "location_dimension.Y": {$gte: -100, $lte: 100}};
    q3b = {"author_dimension.gender": 'female', "location_dimension.X": {$gte: 20, $lte: 40}, "location_dimension.Y": {$gte: -100, $lte: 100}};
    q4a = {"author_dimension.gender":   'male', "time_dimension.full_date": {$gte: startDate, $lte: endDate}, "location_dimension.X": {$gte: 20, $lte: 40}, "location_dimension.Y": {$gte: -100, $lte: 100}};
    q4b = {"author_dimension.gender": 'female', "time_dimension.full_date": {$gte: startDate, $lte: endDate}, "location_dimension.X": {$gte: 20, $lte: 40}, "location_dimension.Y": {$gte: -100, $lte: 100}};

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

    for (i=0; i<1; i++){
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