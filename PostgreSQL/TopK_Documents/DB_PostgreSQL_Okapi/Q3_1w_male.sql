\set startDate '''2015-09-17 00:00:00'''
\set endDate '''2015-09-18 00:00:00'''
\set xStart 20
\set xEnd 40
\set yStart -100
\set yEnd 100
\set gender '''male'''
\set k1 1.6
\set b 0.75
\set top 10
\set words ('''think''')


with 
    q_docLen as (select d.id id, sum(v.count) docLen
                from documents d
                    inner join vocabulary v
                    on v.id_document = d.id
                    inner join documents_authors da
                        inner join authors a
                            inner join genders g
                            on a.id_gender = g.id
                        on da.id_author = a.id
                    on d.id = da.id_document
                    inner join geo_location gl
                    on d.id_geo_loc = gl.id 
                where g.type = :gender
                    and gl.X between :xStart and :xEnd
                    and gl.Y between :yStart and :yEnd
                group by d.id),
    q_wordCountDocs as (select v.id_word id_word, count(distinct v.id_document) wordCountDocs
                        from vocabulary v 
                        where v.id_document in (select d.id
                                                    from documents d
                                                    inner join documents_authors da
                                                        inner join authors a
                                                            inner join genders g
                                                            on a.id_gender = g.id
                                                        on da.id_author = a.id
                                                    on d.id = da.id_document
                                                    inner join geo_location gl
                                                        on d.id_geo_loc = gl.id 
                                                    where g.type = :gender
                                                        and gl.X between :xStart and :xEnd
                                                        and gl.Y between :yStart and :yEnd)
                            group by v.id_word),
    q_noDocs as (select d.id id
                    from documents d
                        inner join documents_authors da
                            inner join authors a
                                inner join genders g
                                on a.id_gender = g.id
                            on da.id_author = a.id
                        on d.id = da.id_document
                        inner join geo_location gl
                        on d.id_geo_loc = gl.id 
                    where g.type = :gender
                        and gl.X between :xStart and :xEnd
                        and gl.Y between :yStart and :yEnd)
select q2.id id, sum(q2.okapi) sokapi 
        from
            (select d.id id, w.word word, -- v.id_word, v.tf, q_dl.docLen, q_wcd.wordCountDocs,
                    ((v.tf * (1 + ln((select count(id) from q_noDocs)::float/q_wcd.wordCountDocs::float)::float)::float * (:k1 + 1))::float/
                    (v.tf + :k1 * (1 - :b + :b * q_dl.docLen / (select avg(docLen) from q_docLen)::float)::float)::float)::float okapi
            from documents d
                inner join vocabulary v
                    inner join words w 
                    on w.id = v.id_word
                on v.id_document = d.id
                inner join documents_authors da
                    inner join authors a
                        inner join genders g
                        on a.id_gender = g.id
                    on da.id_author = a.id
                on d.id = da.id_document
                inner join geo_location gl
                on d.id_geo_loc = gl.id 
                inner join q_docLen q_dl
                on q_dl.id = d.id
                inner join q_wordCountDocs q_wcd
                on q_wcd.id_word =  v.id_word
            where g.type = :gender
                and gl.X between :xStart and :xEnd
                and gl.Y between :yStart and :yEnd
                and w.word in :words) q2
        group by q2.id
        order by 2 desc, 1
        limit :top;

\q



