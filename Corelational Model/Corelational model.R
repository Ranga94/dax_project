constituent_data <- read.csv(file.choose(), header = TRUE, stringsAsFactors = FALSE)
close <- constituent_data$Close
open <- constituent_data$Open
cor.test(close, open, method = "pearson", exact = FALSE)
plot(open,close)
fit <- lm(close ~ open)
abline(fit)
fit
summary(fit)
coefficients <- fit$coefficients
coefficients <- as.data.frame(coefficients)
close_predicted <- 0
for (i in 1: length(close)){
  close_predicted[i] <- coefficients[2,1] * open[i] + coefficients[1,1]
}
plot(close, type= "o", col= "red", main= "Plot of actual and predicted close values")
lines(close_predicted, type="o", col= "blue")

