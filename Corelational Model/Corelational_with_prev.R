bmw <- read.csv(file.choose(), header = TRUE, stringsAsFactors = FALSE) #Read the file from csv
open <- bmw$Open #Extract opening data set
close <- bmw$Close #Extract closing data set
open <- open[-1] #Remove the first row of the data set to synchronize the current opening with the previous closing data 
#close <- close[1:258]
table <- cbind(close, open) #Merge the previous closing and current opening
table <- as.data.frame(table) #Convert it to data frame
open <- table$open #Extract opening values
close <- table$close #Extract closing values

cor.test(close, open, method = "pearson", exact = FALSE) #Carry out corelational test to check if there is any corelation at all

plot(open, close) #plot of close and opening data
fit <- lm(open ~ close) #Best fit line
abline <- fit 
coefficients <- fit$coefficients #Extract the required coefficients from the best fit line
coefficients <- as.data.frame(coefficients) #Convert it to data frame
open_predicted <- 0 #Initialize open_predicted to zero
for (i in 1:length(close)){
  open_predicted[i] <- (coefficients[2,1] * close[i]) + coefficients[1,1]
}
open_predicted <- open_predcited[1:258]
table <- cbind(table, open_predicted)
write.csv(table, file= "previous_pred") #write it to a csv file by name "previous_pred"