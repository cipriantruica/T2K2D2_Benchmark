\set startDate '''2015-09-17 00:00:00'''
\set endDate '''2015-09-18 00:00:00'''
\set startHour 0
\set endHour 23
\set startDay 17
\set endDay 18
\set startMonth 9
\set endMonth 9
\set startYear 2015
\set endYear 2015
\set startX 20
\set endX 40
\set startY -100
\set endY 100
\set gender '''male'''
\set k1 1.6
\set b 0.75
\set top 10
\set words ('''think''','''today''')
\set words ('''think''','''today''','''friday''')

-- top-K keywords Okapi by gender : date  -- OK
with
    q_docLen as (
            select distinct id_document, sum(count) docLen
                from document_facts f 
                    inner join author_dimension ad on ad.id_author = f.id_author
                    inner join time_dimension td on td.id_time = f.id_time
                where gender=:gender
                    and td.full_date between :startDate and :endDate
                group by f.id_document
                )
select wd.word, 
                (1+ln((select count(id_document) from q_docLen)::float/count(distinct f.id_document)::float)::float) 
                    * (:k1 + 1) * 
                    sum(f.tf::float/(f.tf::float + :k1*(1-:b+
                        :b*dl.docLen::float/
                            (select avg(docLen) from q_docLen)::float
                        )))::float Okapi          
            from 
                document_facts f
                inner join author_dimension ad on ad.id_author = f.id_author
                inner join word_dimension wd on wd.id_word = f.id_word
                inner join time_dimension td on td.id_time = f.id_time
                inner join q_docLen dl on dl.id_document = f.id_document
            where
                ad.gender = :gender
                and td.full_date between :startDate and :endDate
            group by wd.word
            order by 2 desc
            limit :top;

\q