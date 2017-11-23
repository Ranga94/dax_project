
#Twitter Analytics

# Organizations people are tweeting about
SELECT constituent_name, entity_tags.ORG, count(entity_tags.ORG) as count FROM [igenie-project:pecten_dataset.tweets] where relevance = 1 and date >= '2017-10-11 00:00:00 UTC' and date < '2011-11-18 00:00:00 UTC' group by constituent_name, entity_tags.ORG order by count DESC LIMIT 10
		 
# Top countries tweeting about a constituent	 
SELECT constituent_name, place.country_code, count(place.country_code) as count FROM [igenie-project:pecten_dataset.tweets] where relevance = 1 and date >= '2017-10-11 00:00:00 UTC' and date < '2011-11-18 00:00:00 UTC' group by constituent_name, place.country_code order by count DESC

# Languages people are Tweeting in
SELECT constituent_name, lang, count(lang) as count FROM [igenie-project:pecten_dataset.tweets] where relevance = 1 and date >= '2017-10-11 00:00:00 UTC' and date < '2011-11-18 00:00:00 UTC' group by lang, constituent_name order by count DESC

# Average Sentiment by Country
SELECT constituent_name, place.country_code, avg(sentiment_score) as avg_sentiment_score FROM [igenie-project:pecten_dataset.tweets] where relevance = 1 and date >= '2017-10-11 00:00:00 UTC' and date < '2011-11-18 00:00:00 UTC'group by constituent_name, place.country_code order by avg_sentiment_score

# What are countries talking about
SELECT constituent_name, place.country_code, entity_tags, count(entity_tags) as count FROM [igenie-project:pecten_dataset.tweets] where relevance = 1, place.country_code IN ('GB', 'US','DE','MX') and date >= '2017-10-11 00:00:00 UTC' and date < '2011-11-18 00:00:00 UTC' group by constituent_name, place.country_code, entity_tags order by count DESC


#News Analytics

#Daily News Sentiment score
SELECT constituent, news_date, AVG(score) as score FROM [igenie-project:pecten_dataset.news] where news_date >= '2017-10-11 00:00:00 UTC' and news_date < '2011-11-18 00:00:00 UTC' group by constituent, news_date order by news_date DESC LIMIT 10

#Sentiment by tags
SELECT constituent, entity_tags, count(entity_tags) as count, score FROM [igenie-project:pecten_dataset.news] where news_date >= '2017-10-11 00:00:00 UTC' and news_date < '2011-11-18 00:00:00 UTC' group by constituent, entity_tags, score 

