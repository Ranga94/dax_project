library(reshape2)
library(tidyverse)
library(plotly)



ADS <- read.csv("directory of correlation dataset")
Twitter_sent = ADS$Twitter_sent
Close = ADS$Close
High = ADS$High
Low = ADS$Low
Open = ADS$Open
News_sent = ADS$News_sent

O <- cor(Twitter_sent, Open)
C <- cor(Twitter_sent, Close)
L <- cor(Twitter_sent, Low)
H <- cor(Twitter_sent, High)

p <- ggplot(ADS, aes(x = date, group=1))

p <- p + {if (C > 0.5) geom_line(aes(y = Close), linetype = "dashed", colour = "black")}
p <- p + {if (O > 0.5) geom_line(aes(y = Open), linetype = "dashed", colour = "red")}
p <- p + {if (H > 0.5) geom_line(aes(y = High), linetype = "dashed", colour = "blue")}
p <- p + {if (L > 0.5) geom_line(aes(y = Low), linetype = "dashed", colour = "orange")}
p <- p + geom_line(aes(y = Twitter_sent*270, colour = "Twitter Sentiment"))
p <- p + theme(axis.title.y=element_blank(), legend.position = c(0.15, 0.850)) + labs(title = "Behaviour of Adidas Prices Relative to Sentiment") 
#p <- p + geom_text(x=2, y=192.2, label="Only Open and High Prices follow the trend of Twitter sentiment ", size=3)
#p <- p + geom_text(x=2, y=191.7, label="from 14/09/2017 to 21/09/2017", size=2.5)
p
ggplotly(p)
p_changed <- ggplotly(p)
pp_changed=plotly_build(p_changed)   
style( pp_changed ) %>% 
  layout( legend = list(x = 0.01, y = 0.95) )

