gc();

##########################################################################################
### Load packages
suppressPackageStartupMessages({
  library(doParallel)
  library(dplyr)  
  library(data.table) 
  library(lubridate)
  library(BBmisc)
  library(caret)  
  library(rstudioapi)
  library(randomForest)
  library(xgboost)      # Extreme Gradient Boosting
})

### set path
path <- rstudioapi::getActiveDocumentContext()$path
setwd(dirname(path))

### Set up for parallel processing
set.seed(1234)
registerDoParallel(3, cores = 3)
getDoParWorkers()



##########################################################################################
# functions

hrs <- function(u) {
  x <- u * 3600
  return(x)
}

mns <- function(m) {
  x <- m * 60
  return(x)
}




##########################################################################################
# what to analyze

CURRENCY_PAIR = "EOSBTC"


##########################################################################################
# read csv input files

filenames <- list.files(CURRENCY_PAIR, pattern="*.csv", full.names=TRUE, recursive = TRUE)
NROW(filenames)



trades = list()
i <- 1
for (fname in filenames) {
  content <- fread(fname)
  cat (fname, " has", NROW(content), " element(s).\n")
  trades[i] <- list(content)
  i = i+1
}

trades <- do.call(rbind,trades)
#typeof(trades)



##########################################################################################
# data preparation

trades<-subset(trades, select = -c(id))
trades<-subset(trades, select = -c(best))

# setting up types
trades$date <- ymd_hms(trades$date)
trades$type <- as.factor(trades$type)

trades <- as.data.table(trades)
setkey(trades, date)
key(trades)


head(trades)
tail(trades)

summary(trades)
NROW(trades)


MAX_SPAN = hrs(1)  #max time span for data


par(mfrow=c(5,1))

from <- ymd_hms(paste(as.character(as.Date(min(trades$date)+hrs(24))),"00:00:00")) + MAX_SPAN #from the beginning of next of min date
len_out <- difftime(max(trades$date), min(trades$date), units = "min")

checkpoints <- seq(from, by = "min", length.out = len_out)

intervals <- c(hrs(1), mns(30), mns(10), mns(5))
max_inv <- max(intervals)

total_cnt <- NROW(checkpoints)
current_progress <- 0

train <- list() #result buffer
trades_smaller <- data.table()
i <- 1

for (cp in checkpoints) {
  #start.time <- Sys.time()
  
  checkpoint = as.POSIXct(cp, origin="1970-01-01")
  train_row = data.table(checkpoint=checkpoint)
  #print (checkpoint)
  
  #end.time <- Sys.time()
  #time.taken <- end.time - start.time
  #print (time.taken)
  
  start.time <- Sys.time()
  
  #make smaller chunk of the 'trades' in every day with the size of day + 1hour(MAX_SPAN). 
  #because the 'trades' object is so huge that the subset takes too much time.
  if ((cp - as.numeric(checkpoints[1])) %% hrs(24) == 0) {
    print ("make trades_smaller")
    print ((cp - as.numeric(checkpoints[1])) %% hrs(24))
    trades_smaller <- trades[trades$date >= (cp-MAX_SPAN-MAX_SPAN) & trades$date < (cp+hrs(24)),]
    print (NROW(trades_smaller))
    print (trades_smaller[1])
    print (trades_smaller[NROW(trades_smaller)])
  }
  
  #trade_chunk <- trades[trades$date >= (cp-max_inv) & trades$date < cp,]
  #print ("make chunk")
  print (as.POSIXct((cp-max_inv), origin="1970-01-01"))
  #print (as.POSIXct((cp), origin="1970-01-01"))
  trade_chunk <- trades_smaller[trades_smaller$date >= (cp-max_inv) & trades_smaller$date < cp,]
  print (NROW(trade_chunk))
  
  end.time <- Sys.time()
  time.taken <- end.time - start.time
  print (time.taken)
  
  trade_chunk$prices_norm <- normalize(trade_chunk$price, method="range")
  trade_chunk$qty_norm <- normalize(trade_chunk$qty, method="range")
  #plot(x=trade_chunk$date, y=trade_chunk$prices_norm)
  
  for (inv in intervals) {
    #start.time <- Sys.time()

    tmp <- trade_chunk[trade_chunk$date >= (cp-inv) & trade_chunk$date < cp,]
    #print (paste(min(tmp$date), "~", max(tmp$date), "(", inv, ")", "has", NROW(tmp), "items"))
    #hist(tmp$prices_norm, main = as.character(inv))
    #trade volume
    size<-NROW(tmp)
    start_price <- tmp$prices_norm[1]
    end_price <- tmp$prices_norm[size]
    x<-table(tmp$type)
    cnt_ask = as.integer(x["ask"])
    cnt_bid = as.integer(x["bid"])
    sum_qty<-sum(tmp$qty)
    sum_ask_qty<-sum(tmp[tmp$type=="ask",]$qty)
    sum_bid_qty<-sum(tmp[tmp$type=="bid",]$qty)
    
    #end.time <- Sys.time()
    #time.taken <- end.time - start.time
    #print (time.taken)

    #start.time <- Sys.time()
    
    #add features
    train_row[,paste0('size_',inv) := size]
    train_row[,paste0('cnt_ask_',inv) := cnt_ask]
    train_row[,paste0('cnt_bid_',inv) := cnt_bid]
    train_row[,paste0('sum_qty_',inv) := sum_qty]
    train_row[,paste0('sum_ask_qty_',inv) := sum_ask_qty]
    train_row[,paste0('sum_bid_qty_',inv) := sum_bid_qty]
    train_row[,paste0('rate_ask_qty_',inv) := ifelse(sum_qty==0,0,sum_ask_qty/sum_qty)]
    train_row[,paste0('rate_bid_qty_',inv) := ifelse(sum_qty==0,0,sum_bid_qty/sum_qty)]
    train_row[,paste0('avg_qty_',inv) := ifelse(size==0,0,sum_qty/size)]
    train_row[,paste0('avg_ask_qty_',inv) := ifelse(cnt_ask==0,0,sum_ask_qty/cnt_ask)]
    train_row[,paste0('avg_bid_qty_',inv) := ifelse(cnt_bid==0,0,sum_bid_qty/cnt_bid)]
    train_row[,paste0('mean_price_',inv) := mean(tmp$prices_norm)]
    train_row[,paste0('sd_price_',inv) := sd(tmp$prices_norm)]
    train_row[,paste0('start_price_',inv) := ifelse(size==0,0,start_price)]
    train_row[,paste0('end_price_',inv) := ifelse(size==0,0,end_price)]
    train_row[,paste0('diff_price_',inv) := ifelse(size==0,0,end_price-start_price)]
    
    #end.time <- Sys.time()
    #time.taken <- end.time - start.time
    #print (time.taken)
  }
  
  train[i] <- list(train_row)

  progress = as.integer(i*100/total_cnt)
  if (current_progress != progress) {
    current_progress = progress
    print (paste("\r\n\r\n", current_progress, "%%\r\n\r\n"))
  }
  i <- i+1
}


train <- do.call(rbind,train)





#train[1440:1450]




#trades[checkpoint>="2017-10-15 19:39:00",]
#trades_smaller


checkpoints <- seq(from, by = "min", length.out = 1440*2+10)#len_out)

hrs(24)
for (cp in checkpoints) {

  #print ((cp - as.numeric(checkpoints[1])))
  #print ((cp - as.numeric(checkpoints[1]))/hrs(24))
  #make smaller chunk of the 'trades' in every day with the size of day + 1hour(MAX_SPAN). 
  #because the 'trades' object is so huge that the subset takes too much time.
  if ((cp - as.numeric(checkpoints[1])) %% hrs(24) == 0) {
    print ("make trades_smaller")
    print ((cp - as.numeric(checkpoints[1])) / hrs(24))
    trades_smaller <- trades[trades$date >= (cp-MAX_SPAN-MAX_SPAN) & trades$date < (cp+hrs(24)),]
    print (NROW(trades_smaller))
    print (trades_smaller[1])
    print (trades_smaller[NROW(trades_smaller)])
  }
}

trades_smaller$date
