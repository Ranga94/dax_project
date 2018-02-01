library(shiny)
library(plotly)
library(bigrquery)
library(lubridate)
project = "igenie-project"

shinyApp(
  ui = fluidPage(
    tabsetPanel(
      tabPanel("Data Collection general statistics", fluid = TRUE,
               sidebarLayout(
                 sidebarPanel(
                   dateRangeInput("dates", "Insert Date range", start = Sys.Date()-7,end = Sys.Date()-1, 
                                  min = "2017-09-11", max = Sys.Date()-1, format = "dd-mm-yyyy", 
                                  separator = " to "),
                   radioButtons("collection_type", "Data type",
                                choices = c("Twitter", "Ticker", "Orbis", "Bloomberg","rss_feeds","stocktwits"),
                                selected = "Twitter"),
                   selectInput("constituent_input", "Constituents",
                               choices = c("ADIDAS AG","ALLIANZ SE","BASF SE","BAYERISCHE MOTOREN WERKE AG","BAYER AG",
                                           "BEIERSDORF AG","COMMERZBANK AKTIENGESELLSCHAFT","CONTINENTAL AG","DAX","DAIMLER AG",
                                           "DEUTSCHE BANK AG","DEUTSCHE POST AG","DEUTSCHE TELEKOM AG","E.ON SE",
                                           "FRESENIUS SE & CO. KGAA","FRESENIUS MEDICAL CARE AG & CO. KGAA","HENKEL AG & CO. KGAA",
                                           "INFINEON TECHNOLOGIES AG","DEUTSCHE LUFTHANSA AG","MUNCHENER RUCKVERSICHERUNGS-GESELLSCHAFT AKTIENGESELLSCHAFT IN MUNCHEN	",
                                           "MERCK KGAA","SAP SE","SIEMENS AG","VOLKSWAGEN AG","VONOVIA SE"))
                   #selectInput("Country", "Select Country", choices = "", selected = "")
                 ),
                 mainPanel(
                   plotOutput("coolplot"),
                   br(),br(),
                   textOutput("mean_var"),
                   br(),
                   textOutput("max_var"),
                   textOutput("min_var")
                 )
               )
      ),
      tabPanel("Data Comparison", fluid = TRUE,
               sidebarLayout(
                 sidebarPanel(
                   selectInput("month_input1","Month 1",
                               choices = c("September 2017","October 2017","November 2017","December 2017", "January 2018")),
                   selectInput("month_input2","Month 2",
                               choices = c("September 2017","October 2017","November 2017","December 2017", "January 2018"),
                               selected = "October 2017"),
                   radioButtons("collection_type1", "Data type",
                                choices = c("Twitter", "Ticker", "Orbis", "Bloomberg","rss_feeds","stocktwits"),
                                selected = "Twitter"),
                   selectInput("constituent_input1", "Constituents",
                               choices = c("ADIDAS AG","ALLIANZ SE","BASF SE","BAYERISCHE MOTOREN WERKE AG",
                                           "BAYER AG", "BEIERSDORF AG","COMMERZBANK AKTIENGESELLSCHAFT",
                                           "CONTINENTAL AG","DAX","DAIMLER AG","DEUTSCHE BANK AG","DEUTSCHE POST AG",
                                           "DEUTSCHE TELEKOM AG","E.ON SE","FRESENIUS SE & CO. KGAA",
                                           "FRESENIUS MEDICAL CARE AG & CO. KGAA","HENKEL AG & CO. KGAA",
                                           "INFINEON TECHNOLOGIES AG","DEUTSCHE LUFTHANSA AG","MUNCHENER RUCKVERSICHERUNGS-GESELLSCHAFT AKTIENGESELLSCHAFT IN MUNCHEN	",
                                           "MERCK KGAA","SAP SE","SIEMENS AG","VOLKSWAGEN AG","VONOVIA SE"))
                 ),
                 mainPanel(
                   
                   plotOutput("comparison_graph")
                   
                   
                 )
               )
      ),
      
      tabPanel("Industry wise comparison", fluid = TRUE,
               sidebarLayout(
                 sidebarPanel(
                   selectInput("Industry_input", "Industries",
                               choices = c("Insurance","Chemicals","Manufacturing",
                                           "Banking","Energy","Medical")),
                   radioButtons("collection_type2", "Data type",
                                choices = c("Twitter","News"), selected = "Twitter")
                 ),
                 mainPanel(
                   textOutput("constituent_display"),
                   br(),
                   plotOutput("Industry_comparison_graph")
                 )
               )))
    
  ), 
  server = function(input, output) {
    output$coolplot <- renderPlot({
      if(input$collection_type == "Twitter"){
        sql <- paste("select constituent_name, date, sum(downloaded_tweets) as tweets FROM [igenie-project:pecten_dataset_test.tweet_logs] where date between TIMESTAMP('", input$dates[1],"') and TIMESTAMP('", input$dates[2],"') and constituent_name = '", input$constituent_input,"' group by constituent_name, date order by date;",sep="")
        retrieved_data <- query_exec(project=project,sql,useLegacySql = FALSE)
        #head(retrieved_data)
        retrieved_data$date <- as.Date(retrieved_data$date,"%d/%m/%Y")
        plot(tweets ~ date, retrieved_data, xlab = "date", ylab = "tweets", type = "b")
        title(paste("Graph of tweets vs date for ",input$constituent_input,sep =''))
        mean <- mean(retrieved_data$tweets)
        output$mean_var <-  renderText(
          paste("The average tweets collected between ",input$dates[1]," and ",input$dates[2]," is ",round(mean, digits = 2), sep="")  
        )
        max <- max(retrieved_data$tweets)
        date_index_max <- which.max(retrieved_data$tweets)
        which_date_max <- retrieved_data$date[date_index_max]
        which_day_max <- weekdays(which_date_max)
        percentage <- ((max - mean)/mean) *100
        output$max_var <- renderText(
          paste("The maximum tweets that were collected for the given date range in a single day is ",max," and was collected on ",which_date_max," (",which_day_max,"), which is ",round(percentage,digit= 2),"% more than the mean.",sep="")
        )
        min <- min(retrieved_data$tweets)
        date_index <- which.min(retrieved_data$tweets)
        which_date <- retrieved_data$date[date_index]
        which_day <- weekdays(which_date)
        percentage_min <- ((mean - min)/mean) *100
        output$min_var <- renderText(
          paste("The minimum tweets that were collected for the given date range in a single day is ",min," and was collected on ",which_date," (",which_day,"), which is ",round(percentage_min, digit =2), "% less than the mean.",sep="")
        )
        
      } else if (input$collection_type == "Ticker"){
        sql <- paste("select constituent_name, date, sum(downloaded_ticks) as ticks FROM [igenie-project:pecten_dataset_test.ticker_logs] where date between TIMESTAMP('", input$dates[1],"') and TIMESTAMP('", input$dates[2],"') and constituent_name = '", input$constituent_input,"' group by constituent_name, date order by date;",sep="")
        retrieved_data <- query_exec(project=project,sql,useLegacySql = FALSE)
        #head(retrieved_data)
        retrieved_data$date <- as.Date(retrieved_data$date,"%d/%m/%Y")
        plot(ticks ~ date, retrieved_data, xlab = "date", ylab = "ticks", type = "b")
        title(paste("Graph of ticks vs date for ",input$constituent_input,sep =''))
        mean <- mean(retrieved_data$ticks)
        output$mean_var <-  renderText(
          paste("The average ticker data collected between ",input$dates[1]," and ",input$dates[2]," is ",mean, sep="")  
          
        )
        max <- max(retrieved_data$ticks)
        date_index_max <- which.max(retrieved_data$ticks)
        which_date_max <- retrieved_data$date[date_index_max]
        which_day_max <- weekdays(which_date_max)
        percentage <- ((max - mean)/mean) *100
        output$max_var <- renderText(
          paste("The maximum ticks that were collected for the given date range on a single day is ",max," and was collected on ",which_date_max," (",which_day_max,"), which is ",round(percentage,digit= 2),"% more than the mean.",sep="")
        )
        min <- min(retrieved_data$ticks)
        date_index <- which.min(retrieved_data$ticks)
        which_date <- retrieved_data$date[date_index]
        which_day <- weekdays(which_date)
        percentage_min <- ((mean - min)/mean) *100
        output$min_var <- renderText(
          paste("The minimum ticks that were collected for the given date range on a single day is ",min," and was collected on ",which_date," (",which_day,"), which is ",round(percentage_min, digit =2), "% less than the mean.",sep="")
        )
      } else if (input$collection_type == "Orbis"){
        sql <- paste("select constituent_name, date, sum(downloaded_news) as orbis FROM [igenie-project:pecten_dataset_test.news_logs] where date between TIMESTAMP('", input$dates[1],"') and TIMESTAMP('", input$dates[2],"') and constituent_name = '", input$constituent_input,"' and source = 'Orbis' group by constituent_name, date order by date;",sep="")
        retrieved_data <- query_exec(project=project,sql,useLegacySql = FALSE)
        #head(retrieved_data)
        retrieved_data$date <- as.Date(retrieved_data$date,"%d/%m/%Y")
        plot(orbis ~ date, retrieved_data, xlab = "date", ylab = "orbis", type = "b")
        title(paste("Graph of orbis vs date for ",input$constituent_input,sep =''))
        mean <- mean(retrieved_data$orbis)
        output$mean_var <-  renderText(
          paste("The average news items collected from orbis between ",input$dates[1]," and ",input$dates[2]," is ",mean, sep="")  
        )
        max <- max(retrieved_data$orbis)
        date_index_max <- which.max(retrieved_data$orbis)
        which_date_max <- retrieved_data$date[date_index_max]
        which_day_max <- weekdays(which_date_max)
        percentage <- ((max - mean)/mean) *100
        output$max_var <- renderText(
          paste("The maximum news itmes collected from orbis for the given date range on a single day is ",max," and was collected on ",which_date_max," (",which_day_max,"), which is ",round(percentage,digit= 2),"% more than the mean.",sep="")
        )
        min <- min(retrieved_data$orbis)
        date_index <- which.min(retrieved_data$orbis)
        which_date <- retrieved_data$date[date_index]
        which_day <- weekdays(which_date)
        percentage_min <- ((mean - min)/mean) *100
        output$min_var <- renderText(
          paste("The minimum news collected from orbis for the given date range in a single day is ",min," and was collected on ",which_date," (",which_day,"), which is ",round(percentage_min, digit =2), "% less than the mean.",sep="")
        )
      } else if (input$collection_type == "Bloomberg"){
        sql <- paste("select constituent_name, date, sum(downloaded_news) as bloomberg FROM [igenie-project:pecten_dataset_test.news_logs] where date between TIMESTAMP('", input$dates[1],"') and TIMESTAMP('", input$dates[2],"') and constituent_name = '", input$constituent_input,"' and source = 'Bloomberg' group by constituent_name, date order by date;",sep="")
        retrieved_data <- query_exec(project=project,sql,useLegacySql = FALSE)
        #head(retrieved_data)
        retrieved_data$date <- as.Date(retrieved_data$date,"%d/%m/%Y")
        plot(bloomberg ~ date, retrieved_data, xlab = "date", ylab = "Bloomberg", type = "b")
        title(paste("Graph of bloomberg vs date for ",input$constituent_input,sep =''))
        mean <- mean(retrieved_data$bloomberg)
        output$mean_var <-  renderText(
          paste("The average news items collectd from bloomberg between ",input$dates[1]," and ",input$dates[2]," is ",mean, sep="")  
        )
        max <- max(retrieved_data$bloomberg)
        date_index_max <- which.max(retrieved_data$bloomberg)
        which_date_max <- retrieved_data$date[date_index_max]
        which_day_max <- weekdays(which_date_max)
        percentage <- ((max - mean)/mean) *100
        output$max_var <- renderText(
          paste("The maximum news collected from Bloomberg for the given date range on a single day is ",max," and was collected on ",which_date_max," (",which_day_max,"), which is ",round(percentage,digit= 2),"% more than the mean.",sep="")
        )
        min <- min(retrieved_data$bloomberg)
        date_index <- which.min(retrieved_data$bloomberg)
        which_date <- retrieved_data$date[date_index]
        which_day <- weekdays(which_date)
        percentage_min <- ((mean - min)/mean) *100
        output$min_var <- renderText(
          paste("The minimum news items that were collected from Bloomberg for the given date range in a single day is ",min," and was collected on ",which_date," (",which_day,"), which is ",round(percentage_min, digit =2), "% less than the mean.",sep="")
        )
      } else if (input$collection_type == "rss_feeds"){
        sql <- paste("select constituent_name, date, sum(downloaded_news) as rss FROM [igenie-project:pecten_dataset_test.news_logs] where date between TIMESTAMP('", input$dates[1],"') and TIMESTAMP('", input$dates[2],"') and constituent_name = '", input$constituent_input,"' and source = 'Yahoo Finance RSS' group by constituent_name, date order by date;",sep="")
        retrieved_data <- query_exec(project=project,sql,useLegacySql = FALSE)
        #head(retrieved_data)
        retrieved_data$date <- as.Date(retrieved_data$date,"%d/%m/%Y")
        plot(rss ~ date, retrieved_data, xlab = "date", ylab = "rss feeds", type = "b")
        title(paste("Graph of rss feeds vs date for ",input$constituent_input,sep =''))
        mean <- mean(retrieved_data$rss)
        output$mean_var <-  renderText(
          paste("The average rss_feeds collected between ",input$dates[1]," and ",input$dates[2]," is ",mean, sep="")  
        )
        max <- max(retrieved_data$rss)
        date_index_max <- which.max(retrieved_data$rss)
        which_date_max <- retrieved_data$date[date_index_max]
        which_day_max <- weekdays(which_date_max)
        percentage <- ((max - mean)/mean) *100
        output$max_var <- renderText(
          paste("The maximum rss feeds that were collected for the given date range in a single day is ",max," and was collected on ",which_date_max," (",which_day_max,"), which is ",round(percentage,digit= 2),"% more than the mean.",sep="")
        )
        min <- min(retrieved_data$rss)
        date_index <- which.min(retrieved_data$rss)
        which_date <- retrieved_data$date[date_index]
        which_day <- weekdays(which_date)
        percentage_min <- ((mean - min)/mean) *100
        output$min_var <- renderText(
          paste("The minimum rss feeds that were collected for the given date range in a single day is ",min," and was collected on ",which_date," (",which_day,"), which is ",round(percentage_min, digit =2), "% less than the mean.",sep="")
        )
      }else if (input$collection_type =="stocktwits"){
        sql <- paste("select constituent_name, date, count(*) as stocktwits FROM [igenie-project:pecten_dataset_test.tweets] where date between TIMESTAMP('", input$dates[1],"') and TIMESTAMP('", input$dates[2],"') and constituent_name = '", input$constituent_input,"' and source = 'StockTwits' group by constituent_name, date order by date;",sep="")
        retrieved_data <- query_exec(project=project,sql,useLegacySql = FALSE)
        #head(retrieved_data)
        retrieved_data$date <- as.Date(retrieved_data$date,"%d/%m/%Y")
        plot(stocktwits ~ date, retrieved_data, xlab = "date", ylab = "rss feeds", type = "b")
        title(paste("Graph of stocktwits vs date for ",input$constituent_input,sep =''))
        mean <- mean(retrieved_data$stocktwits)
        output$mean_var <-  renderText(
          paste("The average stocktwits collected between ",input$dates[1]," and ",input$dates[2]," is ",mean, sep="")  
        )
        max <- max(retrieved_data$stocktwits)
        date_index_max <- which.max(retrieved_data$stocktwits)
        which_date_max <- retrieved_data$date[date_index_max]
        which_day_max <- weekdays(which_date_max)
        percentage <- ((max - mean)/mean) *100
        output$max_var <- renderText(
          paste("The maximum stocktwits that were collected for the given date range for ",input$constituent_input," on a single day is ",max," and was collected on ",which_date_max," (",which_day_max,"), which is ",round(percentage,digit= 2),"% more than the mean.",sep="")
        )
        min <- min(retrieved_data$stocktwits)
        date_index <- which.min(retrieved_data$stocktwits)
        which_date <- retrieved_data$date[date_index]
        which_day <- weekdays(which_date)
        percentage_min <- ((mean - min)/mean) *100
        output$min_var <- renderText(
          paste("The minimum stocktwits that were collected for the given date range for ",input$constituent_input," on a single day is ",min," and was collected on ",which_date," (",which_day,"), which is ",round(percentage_min, digit =2), "% less than the mean.",sep="")
        )
      }
      
      
    })
    output$comparison_graph <- renderPlot({
      if(input$month_input1 == "September 2017"){
        Timestamp11 <- "2017-09-01"
        Timestamp12 <- "2017-09-30"
      } else if(input$month_input1 == "October 2017"){
        Timestamp11 <- "2017-10-01"
        Timestamp12 <- "2017-10-30"
      } else if(input$month_input1 == "November 2017"){
        Timestamp11 <- "2017-11-01"
        Timestamp12 <- "2017-11-30"
      } else if(input$month_input1 == "December 2017"){
        Timestamp11 <- "2017-12-01"
        Timestamp12 <- "2017-12-30"
      } else if(input$month_input1 == "January 2018"){
        Timestamp11 <- "2017-01-01"
        Timestamp12 <- Sys.Date()-1
      }
      if(input$month_input2 == "September 2017"){
        Timestamp21 <- "2017-09-01"
        Timestamp22 <- "2017-09-30"
      } else if(input$month_input2 == "October 2017"){
        Timestamp21 <- "2017-10-01"
        Timestamp22 <- "2017-10-30"
      } else if(input$month_input2 == "November 2017"){
        Timestamp21 <- "2017-11-01"
        Timestamp22 <- "2017-11-30"
      } else if(input$month_input2 == "December 2017"){
        Timestamp21 <- "2017-12-01"
        Timestamp22 <- "2017-12-30"
      } else if(input$month_input2 == "January 2018"){
        Timestamp21 <- "2017-01-01"
        Timestamp22 <- Sys.Date()-1
      }
      if(input$collection_type1 == "Twitter"){
        sql <- paste("select constituent_name, date, sum(downloaded_tweets) as tweets FROM [igenie-project:pecten_dataset_test.tweet_logs] where date between TIMESTAMP('",Timestamp11 ,"') and TIMESTAMP('", Timestamp12,"') and constituent_name = '", input$constituent_input1,"' group by constituent_name, date order by date;",sep="")
        sql1 <- paste("select constituent_name, date, sum(downloaded_tweets) as tweets FROM [igenie-project:pecten_dataset_test.tweet_logs] where date between TIMESTAMP('",Timestamp21 ,"') and TIMESTAMP('", Timestamp22,"') and constituent_name = '", input$constituent_input1,"' group by constituent_name, date order by date;",sep="")
        get_data <- query_exec(project=project,sql,useLegacySql = FALSE)
        get_data1<- query_exec(project=project,sql1,useLegacySql = FALSE)
        #head(retrieved_data)
        get_data$date <- as.Date(get_data$date,"%d/%m/%Y")
        get_data1$date <- as.Date(get_data1$date,"%d/%m/%Y")
        plot(get_data$date,get_data$tweets, xlab = "date", ylab = "tweets", type = "b", col="red")
        lines(get_data$date,get_data1$tweets, col= "blue")
        title(paste("Graph of tweets collected in ",input$month_input1," (red) and ",input$month_input2," (blue)",sep ='')) 
      } else if(input$collection_type1 == "Ticker"){
        sql <- paste("select constituent_name, date, sum(downloaded_ticks) as ticks FROM [igenie-project:pecten_dataset_test.ticker_logs] where date between TIMESTAMP('",Timestamp11 ,"') and TIMESTAMP('", Timestamp12,"') and constituent_name = '", input$constituent_input1,"' group by constituent_name, date order by date;",sep="")
        sql1 <- paste("select constituent_name, date, sum(downloaded_ticks) as ticks FROM [igenie-project:pecten_dataset_test.ticker_logs] where date between TIMESTAMP('",Timestamp21 ,"') and TIMESTAMP('", Timestamp22,"') and constituent_name = '", input$constituent_input1,"' group by constituent_name, date order by date;",sep="")
        get_data <- query_exec(project=project,sql,useLegacySql = FALSE)
        get_data1<- query_exec(project=project,sql1,useLegacySql = FALSE)
        #head(retrieved_data)
        get_data$date <- as.Date(get_data$date,"%d/%m/%Y")
        get_data1$date <- as.Date(get_data1$date,"%d/%m/%Y")
        plot(get_data$date,get_data$ticks, xlab = "date", ylab = "tweets", type = "b", col="red")
        lines(get_data$date,get_data1$ticks, col= "blue")
        title(paste("Graph of ticker data collected in ",input$month_input1," (red) and ",input$month_input2," (blue)",sep ='')) 
      }
    })
    date <- Sys.Date()-3
    month(date) <- month(date) - 5
    a <- date
    date <- Sys.Date() -3
    month(date) <- month(date) - 4
    b <- date
    date <- Sys.Date() -3
    month(date) <- month(date) - 3
    c <- date
    date <- Sys.Date() -3
    month(date) <- month(date) - 2
    d <- date
    date <- Sys.Date() -3
    month(date) <- month(date)-1
    e <- date
    date <- Sys.Date() -3
    month(date) <- month(date)
    f <- date
    dates <- c(a,b,c,d,e,f)
    dates <- format(as.Date(dates),"%Y-%m")
    output$constituent_display <- renderText({
      if (input$Industry_input == "Insurance"){
        paste("The plot includes collection coming from Allianz and Munich Re")}
      
      
      else if(input$Industry_input == "Chemicals"){
        paste("The plot includes collection coming from BASF, Bayer, Beiersdorf, Henkel, Merck")
      }
      else if(input$Industry_input == "Manufacturing"){
        paste("The plot includes collection coming from BMW, Continental, Daimler, Volkswagen,Thyssenkrupp")
      }
      else if(input$Industry_input == "Banking"){
        paste("The plot includes collection coming from Commerzbank and Deutsche Bank")
      }
      else if(input$Industry_input == "Energy"){
        paste("The plot includes collection coming from EON and RWE")
      }	
      else if(input$Industry_input == "Medical"){
        paste("The plot includes collection coming from Fresenisus and Fresenius Mediucal Care")
      }
    }
    
    )
    hold_data <- NULL
    output$Industry_comparison_graph <- renderPlot({
      if(input$collection_type2 == "Twitter" & input$Industry_input == "Insurance"){
        for (year in dates){
          sql <- paste("Select sum(downloaded_tweets) as tweets FROM [igenie-project:pecten_dataset_test.tweet_logs] where date between TIMESTAMP('",year,"-01') and TIMESTAMP('",year,"-30') and constituent_name IN ('ALLIANZ SE','MUNCHENER RUCKVERSICHERUNGS-GESELLSCHAFT AKTIENGESELLSCHAFT IN MUNCHEN')", sep = "")
          get_data <- query_exec(project=project,sql,useLegacySql = FALSE)
          
          
          hold_data <- rbind(hold_data, get_data)}
        merge_data <- cbind(dates, hold_data)
        #print(merge_data)
        barplot(merge_data$tweets, names = dates) 
      }
      else if(input$collection_type2 == "Twitter" & input$Industry_input == "Chemicals"){
        for (year in dates){
          sql <- paste("Select sum(downloaded_tweets) as tweets FROM [igenie-project:pecten_dataset_test.tweet_logs] where date between TIMESTAMP('",year,"-01') and TIMESTAMP('",year,"-30') and constituent_name IN ('BASF SE','BAYER AG','BEIERSDORF AG','HENKEL AG & CO. KGAA','MERCK KGAA')", sep = "")
          get_data <- query_exec(project=project,sql,useLegacySql = FALSE)
          
          
          hold_data <- rbind(hold_data, get_data)}
        merge_data <- cbind(dates, hold_data)
        #print(merge_data)
        barplot(merge_data$tweets, names = dates) 
      }
      else if(input$collection_type2 == "Twitter" & input$Industry_input == "Manufacturing"){
        for (year in dates){
          sql <- paste("Select sum(downloaded_tweets) as tweets FROM [igenie-project:pecten_dataset_test.tweet_logs] where date between TIMESTAMP('",year,"-01') and TIMESTAMP('",year,"-30') and constituent_name IN ('BAYERISCHE MOTOREN WERKE AG','CONTINENTAL AG','DAIMLER AG','VOLKSWAGEN AG','THYSSENKRUPP AG')", sep = "")
          get_data <- query_exec(project=project,sql,useLegacySql = FALSE)
          
          
          hold_data <- rbind(hold_data, get_data)}
        merge_data <- cbind(dates, hold_data)
        #print(merge_data)
        barplot(merge_data$tweets, names = dates) 
      }
      else if(input$collection_type2 == "Twitter" & input$Industry_input == "Banking"){
        for (year in dates){
          sql <- paste("Select sum(downloaded_tweets) as tweets FROM [igenie-project:pecten_dataset_test.tweet_logs] where date between TIMESTAMP('",year,"-01') and TIMESTAMP('",year,"-30') and constituent_name IN ('DEUTSCHE BANK AG','COMMERZBANK AKTIENGESELLSCHAFT')", sep = "")
          get_data <- query_exec(project=project,sql,useLegacySql = FALSE)
          
          
          hold_data <- rbind(hold_data, get_data)}
        merge_data <- cbind(dates, hold_data)
        #print(merge_data)
        barplot(merge_data$tweets, names = dates) 
      }
     else if(input$collection_type2 == "Twitter" & input$Industry_input == "Energy"){
        for (year in dates){
          sql <- paste("Select sum(downloaded_tweets) as tweets FROM [igenie-project:pecten_dataset_test.tweet_logs] where date between TIMESTAMP('",year,"-01') and TIMESTAMP('",year,"-30') and constituent_name IN ('E.ON SE','RWE AG')", sep = "")
          get_data <- query_exec(project=project,sql,useLegacySql = FALSE)
          
          
          hold_data <- rbind(hold_data, get_data)}
        merge_data <- cbind(dates, hold_data)
        #print(merge_data)
        barplot(merge_data$tweets, names = dates) 
      }	  
    })
  







})