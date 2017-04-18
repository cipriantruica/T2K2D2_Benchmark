set serveroutput on
set verify off
SET TERMOUT OFF

define startDate='to_date(''2015-09-17 00:00:00'', ''YYYY-MM-DD HH24:Mi:SS'')'
define endDate='to_date(''2015-09-18 00:00:00'', ''YYYY-MM-DD HH24:Mi:SS'')'
define xStart = 20
define xEnd = 40
define yStart = -100
define yEnd = 100
define gender = '''female'''
define k1=1.6
define b=0.75
define top=10

-- exec sys.flush_pool;

column word format a30
column stfidf format 9999999.9999
set timing on

with 
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
                                group by v.id_word
                            ),
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
    SELECT q1.word, q1.stfidf FROM (
        select q2.word word, sum(q2.tfidf) stfidf 
        from
            (select d.id id, w.word word,
                  v.tf * (1 + ln((select count(id) from q_noDocs )/q_wcd.wordCountDocs)) tfidf
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
                inner join q_wordCountDocs q_wcd
                on q_wcd.id_word =  v.id_word
            where g.type = &gender
                and d.document_date between &startDate and &endDate) q2
            group by q2.word
            order by 2 desc) q1
    WHERE ROWNUM <= &top;

set timing off

exit


