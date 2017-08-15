set serveroutput on
set verify off
SET TERMOUT OFF

define startDate='to_date(''2015-09-17 00:00:00'', ''YYYY-MM-DD HH24:Mi:SS'')'
define endDate='to_date(''2015-09-18 00:00:00'', ''YYYY-MM-DD HH24:Mi:SS'')'
define xStart = 20
define xEnd = 40
define yStart = -100
define yEnd = 100
define gender = '''male'''
define k1=1.6
define b=0.75
define top=10
define words = ('think')

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
                where g.type = &gender
                    and d.document_date between &startDate and &endDate
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
                                                    where g.type = &gender
                                                        and d.document_date between &startDate and &endDate)
                            group by v.id_word),
    q_noDocs as (select d.id id
                    from documents d
                        inner join documents_authors da
                            inner join authors a
                                inner join genders g
                                on a.id_gender = g.id
                            on da.id_author = a.id
                        on d.id = da.id_document
                    where g.type = &gender                        
                        and d.document_date between &startDate and &endDate)
    SELECT q1.id, q1.sokapi FROM (
        select q2.id id, sum(q2.okapi) sokapi 
        from
            (select d.id id, w.word word, -- v.id_word, v.tf, q_dl.docLen, q_wcd.wordCountDocs,
                    (v.tf * (1 + ln((select count(id) from q_noDocs)/q_wcd.wordCountDocs)) * (&k1 + 1))/
                    (v.tf + &k1 * (1 - &b + &b * q_dl.docLen / (select avg(docLen) from q_docLen))) okapi
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
                inner join q_docLen q_dl
                on q_dl.id = d.id
                inner join q_wordCountDocs q_wcd
                on q_wcd.id_word =  v.id_word
            where g.type = &gender
                and d.document_date between &startDate and &endDate
                and w.word in &words) q2
        group by q2.id
        order by 2 desc, 1) q1
    WHERE ROWNUM <= &top;

exit



