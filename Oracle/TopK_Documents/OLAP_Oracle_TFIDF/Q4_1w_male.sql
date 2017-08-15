set serveroutput on
set verify off
SET TERMOUT OFF

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
define xStart = 20
define xEnd = 40
define yStart = -100
define yEnd = 100
define gender = '''male'''
define top=10
define b=0.75
define k1=1.6
define words = ('think')

with
    q_docLen as (
            select distinct f.id_document, sum(f.count) docLen
                from document_facts f 
                    inner join author_dimension ad on ad.id_author = f.id_author
                    inner join time_dimension td on td.id_time = f.id_time
                    inner join location_dimension ld on ld.id_location = f.id_location
                where gender=&gender
                    and td.full_date between &startDate and &endDate
                    and ld.x between &xStart and &xEnd
                    and ld.y between &yStart and &yEnd
                group by f.id_document
            ),
    q_noDocWords as (
            select f.id_word, count(distinct f.id_document) noDocWords 
                from document_facts f
                    inner join author_dimension ad on ad.id_author = f.id_author
                    inner join time_dimension td on td.id_time = f.id_time
                    inner join location_dimension ld on ld.id_location = f.id_location
                where gender=&gender
                    and td.full_date between &startDate and &endDate
                    and ld.x between &xStart and &xEnd
                    and ld.y between &yStart and &yEnd
                group by f.id_word
            )
select * from
(
            select f.id_document,
                sum((1+ln((select count(id_document) from q_docLen)/ndw.noDocWords)) 
                    * f.tf ) TFIDF
            from 
                document_facts f
                inner join word_dimension wd on wd.id_word = f.id_word
                inner join author_dimension ad on ad.id_author = f.id_author
                inner join time_dimension td on td.id_time = f.id_time
                inner join location_dimension ld on ld.id_location = f.id_location
                inner join q_noDocWords ndw on ndw.id_word = f.id_word
            where
                ad.gender = &gender
                and td.full_date between &startDate and &endDate
                and ld.x between &xStart and &xEnd
                and ld.y between &yStart and &yEnd
                and word in &words
            group by f.id_document
            order by 2 desc, 1
)
where rownum<=&top;

exit