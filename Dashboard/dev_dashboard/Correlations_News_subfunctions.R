correlation_news<-function(mydata,constituent){
  title_str = paste('Prices of ',constituent, ' vs. News Sentiments',sep='' )
  
  Close = mydata$Close
  High = mydata$High
  Low = mydata$Low
  Open = mydata$Open
  News_sent = mydata$News_sent
  
  O <- cor(News_sent, Open)
  C <- cor(News_sent, Close)
  L <- cor(News_sent, Low)
  H <- cor(News_sent, High)
  
  ## Avoids constant data affecting the plot.
  if (length(unique(News_sent))==1){
     O=0
     C=0
     L=0
     H=0}
  if (length(unique(Close))<2){C=0}
  if (length(unique(Open))<2){O=0}
  if (length(unique(Low))<2){L=0}
  if (length(unique(High))<2){H=0}
  
  mydata$Date <- as.Date(x = mydata$Date, format = '%d/%m/%Y')
  p <- ggplot(data = mydata, aes(x = Date, group=1))
  p<-p + labs(title = title_str)
  
  
  
  p <- p + {if (C > 0.5) geom_line(aes(y = Close, colour = "Close"), linetype = "dashed")}
  p <- p + {if (O > 0.5) geom_line(aes(y = Open, colour = "Open"), linetype = "dashed")}
  p <- p + {if (H > 0.5) geom_line(aes(y = High, colour = "High"), linetype = "dashed")}
  p <- p + {if (L > 0.5) geom_line(aes(y = Low, colour = 'Low'), linetype = "dashed")}
  
  
  
  ##add annotations and scaling
  ##add annotations and scaling
  if(constituent=='Adidas'){
    p <- p + geom_line(aes(y = News_sent*105, colour = "News Sentiment"))
    p <- p + theme(axis.title.y=element_blank(), legend.position = c(0.15, 0.850)) 
  }
  
  if(constituent=='Allianz'){
    p <- p + geom_line(aes(y = News_sent*550, colour = "News Sentiment"))
    p <- p + theme(axis.title.y=element_blank(), legend.position = c(0.15, 0.850)) 
  }
  
  if(constituent=='Bayer'){
    p <- p + geom_line(aes(y = News_sent*245, colour = "News Sentiment"))
    p <- p + theme(axis.title.y=element_blank(), legend.position = c(0.15, 0.850)) + labs(title = title_str)
  }
  
  if(constituent=='BASF'){
    p <- p + geom_line(aes(y = News_sent*109, colour = "News Sentiment"))
    p <- p + theme(axis.title.y=element_blank(), legend.position = c(0.15, 0.850)) + labs(title = title_str)
  }
  
  if(constituent=='Beiersdorf'){
    p <- p + geom_line(aes(y = News_sent*75, colour = "News Sentiment"))
    p <- p + theme(axis.title.y=element_blank(), legend.position = c(0.15, 0.850)) 
  }
  
  if(constituent =='BMW'){p <- p + geom_line(aes(y = News_sent*69, colour = "News Sentiment"))
  p <- p + theme(axis.title.y=element_blank(), legend.position = c(0.15, 0.850)) 
  }
  
  if(constituent=='Commerzbank'){
    p <- p + geom_line(aes(y = News_sent*9, colour = "News Sentiment"))
    p <- p + theme(axis.title.y=element_blank(), legend.position = c(0.15, 0.850)) 
  }
  
  if(constituent=='Continental'){
    p <- p + geom_line(aes(y = News_sent*273, colour = "News Sentiment"))
    p <- p + theme(axis.title.y=element_blank(), legend.position = c(0.15, 0.850)) 
    #p <- p + geom_text(x=2, y=192.2, label="Only Open and High Prices follow the trend of Twitter sentiment ", size=3)
    #p <- p + geom_text(x=2, y=191.7, label="from 14/09/2017 to 21/09/2017", size=2.5)
  }
  
  
  if(constituent=='Daimler'){
    p <- p + geom_line(aes(y = News_sent*132, colour = "News Sentiment"))
    p <- p + theme(axis.title.y=element_blank(), legend.position = c(0.15, 0.850)) 
  }
  
  
  if(constituent=='Deutsche Börse'){
    p <- p + geom_line(aes(y = News_sent*297, colour = "News Sentiment"))
    p <- p + theme(axis.title.y=element_blank(), legend.position = c(0.15, 0.850))
  }
  
  
  if(constituent=='Deutsche Bank'){
    p <- p + geom_line(aes(y = News_sent*14, colour = "News Sentiment"))
    p <- p + theme(axis.title.y=element_blank(), legend.position = c(0.15, 0.850)) 
  }
  
  if(constituent=='Deutsche Post'){
    p <- p + geom_line(aes(y = News_sent*108, colour = "News Sentiment"))
    p <- p + theme(axis.title.y=element_blank(), legend.position = c(0.15, 0.850)) 
  }
  
  
  if(constituent=="Deutsche Telekom"){
    p <- p + geom_line(aes(y = News_sent*28, colour = "News Sentiment"))
    p <- p + theme(axis.title.y=element_blank(), legend.position = c(0.15, 0.850)) 
  }
  
  
  if(constituent=="EON"){
    p <- p + geom_line(aes(y = News_sent*8.5, colour = "News Sentiment"))
    p <- p + theme(axis.title.y=element_blank(), legend.position = c(0.15, 0.850)) 
  }
  
  if(constituent=='Fresenius Medical Care'){
    p <- p + geom_line(aes(y = News_sent*102, colour = "News Sentiment"))
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
    p <- p + geom_line(aes(y = News_sent*151, colour = "News Sentiment"))
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
    p <- p + geom_line(aes(y = News_sent*314, colour = "News Sentiment"))
    p <- p + theme(axis.title.y=element_blank(), legend.position = c(0.15, 0.850)) 
  }
  
  
  if(constituent=='Merck'){
    p <- p + geom_line(aes(y = News_sent*135, colour = "News Sentiment"))
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
    p <- p + geom_line(aes(y = News_sent*109, colour = "News Sentiment"))
    p <- p + theme(axis.title.y=element_blank(), legend.position = c(0.15, 0.850)) 
  }
  
  if(constituent=='Siemens'){
    p <- p + geom_line(aes(y = News_sent*121, colour = "News Sentiment"))
    p <- p + theme(axis.title.y=element_blank(), legend.position = c(0.15, 0.850)) 
  }
  
  if(constituent=='Thyssenkrupp'){
    p <- p + geom_line(aes(y = News_sent*28, colour = "News Sentiment"))
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
    layout( legend = list(x = 0.999, y = 0.98))
}






#This selects the annotation for News correlation graph
news_annotation_selection<-function(constituent){
  if(constituent=='Adidas'){
    str = 'Open Close, High and Lows have greater than 50% correlation to News sentiment for the total time period shown.'
  }
  if(constituent=='Allianz'){
    str = 'Only Close and Lows have greater than 50% correlation to News sentiment for the total time period shown.'
  }
  if(constituent=='BASF'){
    str = 'All stock prices have less than 50% correlation to News sentiment for the total time period shown.'
  }
  if(constituent=='Bayer'){
    str = 'Only Lows and Open have greater than 50% correlations to News sentiment for the time period shown'
  }
  if(constituent=='Beiersdorf'){
    str = 'All stock prices have less than 50% correlation to News sentiment for the total time period shown.'
  }
  if(constituent=='BMW'){
    str = 'Only High, Close and Low have greater than 50% correlation to News sentiment for the total time period shown.'
  }
  if(constituent=='Commerzbank'){
    str = 'Only Highs have greater than 50% correlation to News sentiment for the total time period shown.'
  }
  if(constituent=='Daimler'){
    str = 'Only Open has greater than 50% correlations to News sentiment for the time period shown.'
  }
  if(constituent=='Deutsche Börse'){
    str = 'Only Close prices have greater than 50% correlations to News sentiment for the time period shown.'
  }
  if(constituent=='Deutsche Bank'){
    str = 'All stock prices have less than 50% correlation to News sentiment for the total time period shown.'
  }
  if(constituent=='Deutsche Post'){
    str = 'Only Close prices have greater than 50% correlation to News sentiment for the total time period shown.'
  }
  if(constituent=='Deutsche Telekom'){
    str = 'Only High, Close and Low have greater than 50% correlation to News sentiment for the total time period shown.'
  }
  if(constituent=='EON'){
    str ='Only Open and Close prices have greater than 50% correlation to News sentiment for the total time period shown.'
  }
  if(constituent=='Fresenius Medical Care'){
    str = 'Open Close, High and Lows have greater than 50% correlation to News sentiment for the total time period shown.'
  }
  if(constituent=='Fresenius'){
    str = 'All stock prices have less than 50% correlation to News sentiment for the total time period shown.'
  }
  if(constituent=='Heidelberg Cement'){
    str = 'All stock prices have less than 50% correlation to News sentiment for the total time period shown.'
  }
  if(constituent=='Henkel'){
    str = 'Open Close and Lows have greater than 50% correlations to News sentiment for Henkel.'
  }
  if(constituent=='Infineon'){
    str = 'Only Open prices have greater than 50% correlation to News sentiment for the total time period shown.'
  }
  if(constituent=='Lufthansa'){
    str = 'All stock prices have less than 50% correlation to News sentiment for the total time period shown.'
  }
  if(constituent=='Linde'){
    str = 'Only Open Prices have greater than 50% correlations to News sentiment for the time period shown.'
  }
  if(constituent=='Merck'){
    str = 'Only Open and Low prices have greater than 50% correlation to News sentiment for the total time period shown.'
  }
  if(constituent=='Prosiebensat1 Media'){
    str = 'All stock prices have less than 50% correlation to News sentiment for the total time period shown.'
  }
  if(constituent=='RWE'){
    str = 'All stock prices have less than 50% correlation to News sentiment for the total time period shown.'
  }
  if(constituent=='SAP'){
    str = 'Low, Close and High Prices have greater than 50% correlation to News sentiment for the total time period shown.'
  }
  if(constituent=='Siemens'){
    str = 'All stock prices have less than 50% correlation to News sentiment for the total time period shown.'
  }
  if(constituent=='Thyssenkrupp'){
    str = 'Low, Open and High prices have greater than 50% correlation to News sentiment for the total time period shown.'
  }
  if(constituent=='Vonovia'){
    str = 'All stock prices have less than 50% correlation to News sentiment for the total time period shown.'
  }
  if(constituent=='Volkswagen'){
    str = 'All stock prices have less than 50% correlation to News sentiment for the total time period shown.'
  }
  if(constituent=='Continental'){
    str = 'Only Low prices have greater than 50% correlation to News sentiment for the total time period shown.'
  }
  str
}