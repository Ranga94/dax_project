correlation_news<-function(mydata,constituent){
  title_str = paste('Behavior of ',constituent, ' prices relative to News Sentiments',sep='' )
  
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
  p <- ggplot(mydata, aes(x = date, group=1))
  
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
    p <- p + theme(axis.title.y=element_blank(), legend.position = c(0.15, 0.850)) + labs(title = "Behaviour of Allianz Prices Relative to Sentiment")
     }
  
  if(constituent=='Commerzbank'){
    p <- p + geom_line(aes(y = News_sent*70, colour = "News Sentiment"))
    p <- p + theme(axis.title.y=element_blank(), legend.position = c(0.15, 0.850)) + labs(title = title_str) 
  }
  
  if(constituent=='Deutsche Bank'){
    p <- p + geom_line(aes(y = News_sent*270, colour = "News Sentiment"))
    p <- p + theme(axis.title.y=element_blank(), legend.position = c(0.15, 0.850)) + labs(title = title_str) 
  }
  
  if(constituent=='EON'){
    p <- p + geom_line(aes(y = News_sent*56, colour = "News Sentiment"))
    p <- p + theme(axis.title.y=element_blank(), legend.position = c(0.15, 0.850)) + labs(title =title_str) 
  }
  #p <- p + theme(plot.title=element_text(size=10))
  #ggplotly(p)
  #p_changed <- ggplotly(p)
  #pp_changed=plotly_build(p_changed)   
  #style( pp_changed ) %>% 
   # layout( legend = list(x = 0.01, y = 0.95) )}
  p
  ggplotly(p)
  p_changed <- ggplotly(p)
  pp_changed=plotly_build(p_changed)   
  style( pp_changed ) %>% 
    layout( legend = list(x = 0.01, y = 0.95) )
}






#This selects the annotation for News correlation graph
news_annotation_selection<-function(constituent){
  if(constituent=='Adidas'){
    str = "Only Highs, Open and Lows have greater than 50% correlations to News sentiment"
  }
  if(constituent=='BMW'){
    str = "All BMW stock prices have less than 50% correlation to News sentiment for the total time period shown."
  }
  if(constituent=='Commerzbank'){
    str = "Only Close prices have greater than 50% correlation to News sentiment for the total time period shown."
  }
  if(constituent=='Deutsche Bank'){
    str = "Only Open prices have greater than 50% correlation to News sentiment for the total time period shown."
  }
  if(constituent=='EON'){
    str = "Highs, Close, Open and Lows all have greater than 50% correlations to News sentiment."
  }
  str
}


