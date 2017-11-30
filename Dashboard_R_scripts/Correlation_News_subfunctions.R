correlation_news<-function(mydata,constituent){
  title_str = paste('Behavior of ',constituent, ' Prices Relative to News Sentiments',sep='' )
  
  Close = mydata$Close
  High = mydata$High
  Low = mydata$Low
  Open = mydata$Open
  News_sent = mydata$News_sent
  
  O <- cor(News_sent, Open)
  C <- cor(News_sent, Close)
  L <- cor(News_sent, Low)
  H <- cor(News_sent, High)
  
  mydata$Date <- as.Date(x = mydata$Date, format = '%d/%m/%Y')
  p <- ggplot(data = mydata, aes(x = Date, group=1))
  p<-p + labs(title = title_str)
  
  
  p <- p + {if (C > 0.5) geom_line(aes(y = Close), linetype = "dashed", colour = "black")}
  p <- p + {if (O > 0.5) geom_line(aes(y = Open), linetype = "dashed", colour = "red")}
  p <- p + {if (H > 0.5) geom_line(aes(y = High), linetype = "dashed", colour = "blue")}
  p <- p + {if (L > 0.5) geom_line(aes(y = Low), linetype = "dashed", colour = "orange")}
  
  
  ##add annotations and scaling
  if(constituent=='Adidas'){
    p <- p + geom_line(aes(y = News_sent*170, colour = "News Sentiment"))
  }
  
  if(constituent=='Allianz'){
    p <- p + geom_line(aes(y = News_sent*190, colour = "News Sentiment"))
    p <- p + theme(axis.title.y=element_blank(), legend.position = c(0.15, 0.850)) 
     }
  
  if(constituent=='Bayer'){
    p <- p + geom_line(aes(y = News_sent*175, colour = "News Sentiment"))
    p <- p + theme(axis.title.y=element_blank(), legend.position = c(0.15, 0.850)) + labs(title = title_str)
    }
  
  if(constituent=='BASF'){
    p <- p + geom_line(aes(y = News_sent*96, colour = "News Sentiment"))
    p <- p + theme(axis.title.y=element_blank(), legend.position = c(0.15, 0.850)) + labs(title = title_str)
    }
  
  if(constituent=='Beiersdorf'){
    p <- p + geom_line(aes(y = News_sent*75, colour = "News Sentiment"))
    p <- p + theme(axis.title.y=element_blank(), legend.position = c(0.15, 0.850)) 
  }
  
  if(constituent =='BMW'){p <- p + geom_line(aes(y = News_sent*85, colour = "News Sentiment"))
  p <- p + theme(axis.title.y=element_blank(), legend.position = c(0.15, 0.850)) 
  }
  
  if(constituent=='Commerzbank'){
    p <- p + geom_line(aes(y = News_sent*11, colour = "News Sentiment"))
    p <- p + theme(axis.title.y=element_blank(), legend.position = c(0.15, 0.850)) 
  }
  
  if(constituent=='Continental'){
    p <- p + geom_line(aes(y = News_sent*200, colour = "News Sentiment"))
    p <- p + theme(axis.title.y=element_blank(), legend.position = c(0.15, 0.850)) 
    #p <- p + geom_text(x=2, y=192.2, label="Only Open and High Prices follow the trend of Twitter sentiment ", size=3)
    #p <- p + geom_text(x=2, y=191.7, label="from 14/09/2017 to 21/09/2017", size=2.5)
  }
  
  
  if(constituent=='Daimler'){
    p <- p + geom_line(aes(y = News_sent*178, colour = "News Sentiment"))
    p <- p + theme(axis.title.y=element_blank(), legend.position = c(0.15, 0.850)) 
  }
  
  
  if(constituent=='Deutsche Borse'){
    p <- p + geom_line(aes(y = News_sent*87, colour = "News Sentiment"))
    p <- p + theme(axis.title.y=element_blank(), legend.position = c(0.15, 0.850))
  }
  
  
  if(constituent=='Deutsche Bank'){
    p <- p + geom_line(aes(y = News_sent*14, colour = "News Sentiment"))
    p <- p + theme(axis.title.y=element_blank(), legend.position = c(0.15, 0.850)) 
  }
  
  if(constituent=='Deutsche Post'){
    p <- p + geom_line(aes(y = News_sent*38, colour = "News Sentiment"))
    p <- p + theme(axis.title.y=element_blank(), legend.position = c(0.15, 0.850)) 
  }
  
  
  if(constituent=="Deutsche Telekom"){
    p <- p + geom_line(aes(y = News_sent*15, colour = "News Sentiment"))
    p <- p + theme(axis.title.y=element_blank(), legend.position = c(0.15, 0.850)) 
  }
  
  
  if(constituent=="EON"){
    p <- p + geom_line(aes(y = News_sent*9.5, colour = "News Sentiment"))
    p <- p + theme(axis.title.y=element_blank(), legend.position = c(0.15, 0.850)) 
  }
  
  if(constituent=='Fresenius Medical Care'){
    p <- p + geom_line(aes(y = News_sent*82, colour = "News Sentiment"))
    p <- p + theme(axis.title.y=element_blank(), legend.position = c(0.15, 0.850)) 
  }
  
  if(constituent=='Fresenius'){
    p <- p + geom_line(aes(y = News_sent*70, colour = "News Sentiment"))
    p <- p + theme(axis.title.y=element_blank(), legend.position = c(0.15, 0.850)) 
  }
  
  
  if(constituent=='Heidelberg Cement'){
    p <- p + geom_line(aes(y = News_sent*85, colour = "News Sentiment"))
    p <- p + theme(axis.title.y=element_blank(), legend.position = c(0.15, 0.850)) 
  }
  
  if(constituent=='Henkel'){
    p <- p + geom_line(aes(y = News_sent*145, colour = "News Sentiment"))
    p <- p + theme(axis.title.y=element_blank(), legend.position = c(0.15, 0.850)) 
  }
  
  
  if(constituent=='Infineon'){
    p <- p + geom_line(aes(y = News_sent*23, colour = "News Sentiment"))
    p <- p + theme(axis.title.y=element_blank(), legend.position = c(0.15, 0.850)) 
  }
  
  if(constituent=='Lufthansa'){
    p <- p + geom_line(aes(y = News_sent*26, colour = "News Sentiment"))
    p <- p + theme(axis.title.y=element_blank(), legend.position = c(0.15, 0.850)) 
  }
  
  if(constituent=='Linde'){
    p <- p + geom_line(aes(y = News_sent*324, colour = "News Sentiment"))
    p <- p + theme(axis.title.y=element_blank(), legend.position = c(0.15, 0.850)) 
  }
  
  
  if(constituent=='Merck'){
    p <- p + geom_line(aes(y = News_sent*95, colour = "News Sentiment"))
    p <- p + theme(axis.title.y=element_blank(), legend.position = c(0.15, 0.850)) 
  }
  
  
  if(constituent=='ProSiebenSat1 Media'){
    p <- p + geom_line(aes(y = News_sent*29, colour = "News Sentiment"))
    p <- p + theme(axis.title.y=element_blank(), legend.position = c(0.15, 0.850)) 
  }
  
  
  if(constituent=='RWE'){
    p <- p + geom_line(aes(y = News_sent*21, colour = "News Sentiment"))
    p <- p + theme(axis.title.y=element_blank(), legend.position = c(0.15, 0.850))
  }
  
  if(constituent=='SAP'){
    p <- p + geom_line(aes(y = News_sent*96, colour = "News Sentiment"))
    p <- p + theme(axis.title.y=element_blank(), legend.position = c(0.15, 0.850)) 
  }
  
  if(constituent=='Siemens'){
    p <- p + geom_line(aes(y = News_sent*121, colour = "News Sentiment"))
    p <- p + theme(axis.title.y=element_blank(), legend.position = c(0.15, 0.850)) 
  }
  
  if(constituent=='Thyssenkrupp'){
    p <- p + geom_line(aes(y = News_sent*13, colour = "News Sentiment"))
    p <- p + theme(axis.title.y=element_blank(), legend.position = c(0.15, 0.850))
  }
  
  if(constituent=='Vonovia'){
    p <- p + geom_line(aes(y = News_sent*36, colour = "News Sentiment"))
    p <- p + theme(axis.title.y=element_blank(), legend.position = c(0.15, 0.850))
  }
  
  if(constituent=='Volkswagen'){
    p <- p + geom_line(aes(y = News_sent*620, colour = "News Sentiment"))
    p <- p + theme(axis.title.y=element_blank(), legend.position = c(0.15, 0.850)) 
  }
  
  #p <- p + theme(plot.title=element_text(size=10))
  #ggplotly(p)
  #p_changed <- ggplotly(p)
  #pp_changed=plotly_build(p_changed)   
  #style( pp_changed ) %>% 
   # layout( legend = list(x = 0.01, y = 0.95) )}
  p <- p + theme(plot.title=element_text(size=10))+theme(plot.title = element_text(lineheight=.6, face="bold"))
  ggplotly(p)
  p_changed <- ggplotly(p)
  pp_changed=plotly_build(p_changed)   
  style( pp_changed ) %>% 
    layout( legend = list(x = 0.01, y = 0.95) )
}






#This selects the annotation for News correlation graph
news_annotation_selection<-function(constituent){
  if(constituent=='Adidas'){
    str = 'All stock prices have less than 50% correlation to News sentiment for the total time period shown.'
  }
  if(constituent=='Allianz'){
    str = 'All stock prices have less than 50% correlation to News sentiment for the total time period shown.'
  }
  if(constituent=='BASF'){
    str = 'All stock prices have less than 50% correlation to News sentiment for the total time period shown.'
  }
  if(constituent=='Bayer'){
    str = 'Open Close, High and Lows have greater than 50% correlations to News sentiment for the time period shown'
  }
  if(constituent=='Beiersdorf'){
    str = 'All stock prices have less than 50% correlation to News sentiment for the total time period shown.'
  }
  if(constituent=='BMW'){
    str = 'All stock prices have less than 50% correlation to News sentiment for the total time period shown.'
  }
  if(constituent=='Commerzbank'){
    str = 'All stock prices have less than 50% correlation to News sentiment for the total time period shown.'
  }
  if(constituent=='Daimler'){
    str = 'Open Close, High and Lows have greater than 50% correlations to Twitter sentiment for the time period shown'
  }
  if(constituent=='Deutsche_Boerse'){
    str = 'Open Close, High and Lows have greater than 50% correlations to Twitter sentiment for the time period shown'
  }
  if(constituent=='Deutsche_Bank'){
    str = 'All stock prices have less than 50% correlation to News sentiment for the total time period shown.'
  }
  if(constituent=='Deutsche_Post'){
    str = 'All stock prices have less than 50% correlation to News sentiment for the total time period shown.'
  }
  if(constituent=='Deutsche_Telekom'){
    str = 'All stock prices have less than 50% correlation to News sentiment for the total time period shown.'
  }
  if(constituent=='EON'){
    str ='All stock prices have less than 50% correlation to News sentiment for the total time period shown.'
  }
  if(constituent=='Fresenius_Medical_Care'){
    str = 'All stock prices have less than 50% correlation to News sentiment for the total time period shown.'
  }
  if(constituent=='Fresenius'){
    str = 'All stock prices have less than 50% correlation to News sentiment for the total time period shown.'
  }
  if(constituent=='Heidelberg_Cement'){
    str = 'All stock prices have less than 50% correlation to News sentiment for the total time period shown.'
  }
  if(constituent=='Henkel'){
    str = 'Open Close, High and Lows have greater than 50% correlations to Twitter sentiment for Henkel'
  }
  if(constituent=='Infineon'){
    str = 'All stock prices have less than 50% correlation to News sentiment for the total time period shown.'
  }
  if(constituent=='Lufthansa'){
    str = 'All stock prices have less than 50% correlation to News sentiment for the total time period shown.'
  }
  if(constituent=='Linde'){
    str = 'Only Open Prices have greater than 50% correlations to Twitter sentiment for the time period shown'
  }
  if(constituent=='Merck'){
    str = 'All stock prices have less than 50% correlation to News sentiment for the total time period shown.'
  }
  if(constituent=='Prosiebensat1_Media'){
    str = 'All stock prices have less than 50% correlation to News sentiment for the total time period shown.'
  }
  if(constituent=='RWE'){
    str = 'All stock prices have less than 50% correlation to News sentiment for the total time period shown'
  }
  if(constituent=='SAP'){
    str = 'All stock prices have less than 50% correlation to News sentiment for the total time period shown.'
  }
  if(constituent=='Siemens'){
    str = 'All stock prices have less than 50% correlation to News sentiment for the total time period shown.'
  }
  if(constituent=='Thyssenkrupp'){
    str = 'All stock prices have less than 50% correlation to News sentiment for the total time period shown.'
  }
  if(constituent=='Vonovia'){
    str = 'All stock prices have less than 50% correlation to News sentiment for the total time period shown.'
  }
  if(constituent=='Volkswagen'){
    str = 'Open Close, High and Lows have greater than 50% correlations to Twitter sentiment for the time period shown'
  }
  if(constituent=='Continental'){
    str = 'All stock prices have less than 50% correlation to News sentiment for the total time period shown.'
  }
  str
}


