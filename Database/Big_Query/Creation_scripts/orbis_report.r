library(bigrquery)

month = c(1,2,3,4,5,6,7,8,9,10,11,12)
year = C(1991,1992,1993,1994,1995,1996,1997,1998,1999,2000,2001,2002,2003,2004,2005,2006,2007,2008,2009,2010,2011,2012,2013,2014,2015,2016,2017,2018)

project <- "igenie-project"
sql <- "SELECT constituent_name, count(constituent_name) as count 
FROM [igenie-project:pecten_dataset.all_news]
where news_date between TIMESTAMP('2017-02-01') and TIMESTAMP('2017-02-31')
group by constituent_name"
month_data <- query_exec(sql, project = project, useLegacySql = FALSE)
count <- nrow(month_data)
month <- rep(x,each=count)


