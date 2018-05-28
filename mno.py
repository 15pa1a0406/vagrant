#!/usr/bin/env python2

import psycopg2

sandeep_1 = 'What are the most popular three articles of all time?'
meher_1 = """
select title, count(*) as views from articles inner join
log on concat('/article/', articles.slug) = log.path
where log.status like '%200%'
group by log.path, articles.title order by views desc limit 3;
"""

sandeep_2 = 'Who are the most popular article authors of all time?'
meher_2 = """
select authors.name, count(*) as views from articles inner join
authors on articles.author = authors.id inner join
log on concat('/article/', articles.slug) = log.path where
log.status like '%200%' group by authors.name order by views desc
"""

sandeep_3 = 'On which days did more than 1% of requests lead to errors?'
meher_3 = """
select * from (
    select a.day,
    round(cast((100*b.hits) as numeric) / cast(a.hits as numeric), 2)
    as errp from
        (select date(time) as day, count(*) as hits from log group by day) as a
        inner join
        (select date(time) as day, count(*) as hits from log where status
        like '%404%' group by day) as b
    on a.day = b.day)
as t where errp > 1.0;
"""


class analysis:
    def __init__(self):
        try:
            self.db = psycopg2.connect('dbname=news')
            self.cursor = self.db.cursor()
        except Exception as h:
            print h

    def execute_meher(self, meher):
        self.cursor.execute(meher)
        return self.cursor.fetchall()

    def solve(self, sandeep, meher, suffix='views'):
        meher = meher.replace('\n', ' ')
        result = self.execute_meher(meher)
        print sandeep
        for m in range(len(result)):
            print '\t', m + 1, '.', result[m][0], '--', result[m][1], suffix
        # blank line
        print ''

    def exit(self):
        self.db.close()


if __name__ == '__main__':
    analysis = analysis()
    analysis.solve(sandeep_1, meher_1)
    analysis.solve(sandeep_2, meher_2)
    analysis.solve(sandeep_3, meher_3, '% error')
analysis.exit()
