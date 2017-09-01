test <- read.csv(file.choose(), header = TRUE, stringsAsFactors = FALSE) #Reading the data from csv
close <- test$Close #Extract closing data
open <- test$Open #Extract opening Data
high <- test$High #Extract High data
low <- test$Low #Extract Low data

table <- cbind(close, open, high, low) #Combine the four data to form a table
table <- as.data.frame(table) #Convert it to data frame
fit <- lm(table) #Fit the data 

coefficients <- fit$coefficients #Extract the necessary coefficients from the best fit line
coefficients <- as.data.frame(coefficients) #Convert the data to dataframe for further analysis

close_predicted <- 0 #Initialse Close_predicted value to zero
for (i in 1: length(close)){
  close_predicted[i] <- coefficients[1,1]+(coefficients[2,1]*open[i])+(coefficients[3,1]*high[i]+(coefficients[4,1]*low[i]))
}

close_predicted
