set serveroutput on
set verify off
set termout off
set echo off

define startDate='to_date(''2015-09-17 00:00:00'', ''YYYY-MM-DD HH24:Mi:SS'')'
define endDate='to_date(''2015-09-18 00:00:00'', ''YYYY-MM-DD HH24:Mi:SS'')'
define startHour=0
define endHour=23
define startDay=17
define endDay=18
define startMonth=9
define endMonth=9
define startYear=2015
define endYear=2015
define startX = 20
define endX = 40
define startY = -100
define endY = 100
define gender = '''female'''
define top=10
define b=0.75
define k1=1.6
define words = ('think')
define words = ('think','today')
define words = ('think','today','friday')

-- top-K keywords Okapi by gender - ok
with
    q_docLen as (
            select distinct id_document, sum(count) docLen
                from document_facts f 
                    inner join author_dimension ad on ad.id_author = f.id_author
                where gender=&gender
                group by f.id_document
                )
select * from
(
            select wd.word, 
                (1+ln((select count(id_document) from q_docLen)/count(distinct f.id_document))) 
                    * (&k1 + 1) * 
                    sum(f.tf/(f.tf + &k1*(1-&b+
                        &b*dl.docLen/
                            (select avg(docLen) from q_docLen)
                        ))) Okapi          
            from 
                document_facts f
                inner join word_dimension wd on wd.id_word = f.id_word
                inner join author_dimension ad on ad.id_author = f.id_author
                inner join q_docLen dl on dl.id_document = f.id_document
            where
                ad.gender = &gender
            group by wd.word
            order by 2 desc
)
where rownum<=&top;

exit