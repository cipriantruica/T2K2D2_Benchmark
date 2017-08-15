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

-- TFIDF by gender & date & location - ok
with
    noDocs as (select distinct id_document
                from document_facts f 
                    inner join author_dimension ad on ad.id_author = f.id_author
                    inner join location_dimension ld on ld.id_location = f.id_location
                    inner join time_dimension td on td.id_time = f.id_time
                where ad.gender=&gender
                    and ld.x between &startX and &endX
                    and ld.y between &startY and &endY
                    and td.full_date between &startDate and &endDate
                )
select word, TFIDF from
(
    select wd.word, sum(f.tf) * (1+ln((select count(id_document) from noDocs)/count(distinct f.id_document))) TFIDF
    from 
        document_facts f
        inner join word_dimension wd on wd.id_word = f.id_word
        inner join author_dimension ad on ad.id_author = f.id_author
        inner join time_dimension td on td.id_time = f.id_time
        inner join location_dimension ld on ld.id_location = f.id_location
    where
        ad.gender = &gender
        and ld.x between &startX and &endX
        and ld.y between &startY and &endY
        and td.full_date between &startDate and &endDate
    group by wd.word
    order by 2 desc
)
where rownum<=&top;

exit