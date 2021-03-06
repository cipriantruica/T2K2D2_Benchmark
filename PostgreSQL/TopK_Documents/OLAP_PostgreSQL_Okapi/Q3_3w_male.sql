\set startDate '''2015-09-17 00:00:00'''
\set endDate '''2015-09-18 00:00:00'''
\set xStart 20
\set xEnd 40
\set yStart -100
\set yEnd 100
\set gender '''male'''
\set top 10
\set b 0.75
\set k1 1.6
\set words ('''think''','''today''','''friday''')


with
    q_docLen as (
            select distinct f.id_document, sum(f.count) docLen
                from document_facts f 
                    inner join author_dimension ad on ad.id_author = f.id_author
                    inner join location_dimension ld on ld.id_location = f.id_location
                where gender=:gender
                    and ld.x between :xStart and :xEnd
                    and ld.y between :yStart and :yEnd
                group by f.id_document
            ),
    q_noDocWords as (
            select f.id_word, count(distinct f.id_document) noDocWords 
                from document_facts f
                    inner join author_dimension ad on ad.id_author = f.id_author
                    inner join location_dimension ld on ld.id_location = f.id_location
                where gender=:gender
                    and ld.x between :xStart and :xEnd
                    and ld.y between :yStart and :yEnd
                group by f.id_word
            )
select f.id_document,
                sum((1+ln((select count(id_document) from q_docLen)::float/ndw.noDocWords)::float)::float 
                    * (:k1 + 1) * 
                    (f.tf/(f.tf + :k1*(1-:b+
                        :b*dl.docLen/
                            (select avg(docLen) from q_docLen)::float
                        )::float)::float)::float)::float Okapi          
            from 
                document_facts f
                inner join word_dimension wd on wd.id_word = f.id_word
                inner join author_dimension ad on ad.id_author = f.id_author
                inner join location_dimension ld on ld.id_location = f.id_location
                inner join q_docLen dl on dl.id_document = f.id_document
                inner join q_noDocWords ndw on ndw.id_word = f.id_word
            where
                ad.gender = :gender
                and ld.x between :xStart and :xEnd
                and ld.y between :yStart and :yEnd
                and word in :words
            group by f.id_document
            order by 2 desc, 1
            limit :top;

\q