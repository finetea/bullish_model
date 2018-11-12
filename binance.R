gc();

##########################################################################################
### Load packages
suppressPackageStartupMessages({
  library(doParallel)
  library(parallel)
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

no_cores <- detectCores() - 1
no_cores  

registerDoParallel(no_cores, cores = no_cores)
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


start.time <- Sys.time()

trades = list()
i <- 1
for (fname in filenames) {
  content <- fread(fname)
  cat (fname, " has", NROW(content), " element(s).\n")
  trades[i] <- list(content)
  i = i+1
}
rm(content)

trades <- do.call(rbind,trades)
#typeof(trades)


end.time <- Sys.time()
time.taken <- end.time - start.time
print (paste(time.taken, "for file loading"))

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


##########################################################################################
# create features


MAX_SPAN = hrs(1)  #max time span for data


par(mfrow=c(5,1))

from <- ymd_hms(paste(as.character(as.Date(min(trades$date)+hrs(24))),"00:00:00")) + MAX_SPAN #from the beginning of next of min date
len_out <- difftime(max(trades$date), from, units = "min")

checkpoints <- seq(from, by = "min", length.out = len_out)
NROW(checkpoints)



start.time.total <- Sys.time()


intervals <- c(hrs(1), mns(30), mns(10), mns(5))
max_inv <- max(intervals)

total_cnt <- NROW(checkpoints)
current_progress <- 0

train <- list() #result buffer
trades_smaller <- data.table()
i <- 1



for (cp in checkpoints) {
  start.time.sub <- Sys.time()
  
  start.time <- Sys.time()
  
  checkpoint = as.POSIXct(cp, origin="1970-01-01")
  #train_row = data.table(checkpoint=checkpoint)
  train_row = c(checkpoint)
  #print (checkpoint)
  
  end.time <- Sys.time()
  time.taken <- end.time - start.time
  #print (paste(time.taken, "for init train_row OK"))
  
  start.time <- Sys.time()
  
  #make smaller chunk of the 'trades' in every day with the size of day + 1hour(MAX_SPAN). 
  #because the 'trades' object is so huge that the subset takes too much time.
  size_smaller = hrs(24)
  if ((cp - as.numeric(checkpoints[1])) %% size_smaller == 0) {
    print ("make trades_smaller")
    print ((cp - as.numeric(checkpoints[1])) %% size_smaller)
    trades_smaller <- trades[trades$date >= (cp-MAX_SPAN-MAX_SPAN) & trades$date < (cp+size_smaller),]
    print (NROW(trades_smaller))
    print (trades_smaller[1])
    print (trades_smaller[NROW(trades_smaller)])
  }
  
  #trade_chunk <- trades[trades$date >= (cp-max_inv) & trades$date < cp,]
  #print (as.POSIXct((cp), origin="1970-01-01"))
  trade_chunk <- trades_smaller[trades_smaller$date >= (cp-max_inv) & trades_smaller$date < cp,]
  #print (paste0("make chunk", as.character(NROW(trade_chunk))))
  
  end.time <- Sys.time()
  time.taken <- end.time - start.time
  #print (paste(time.taken, "for making trade_chunk"))
  
  start.time <- Sys.time()

  trade_chunk$prices_norm <- normalize(trade_chunk$price, method="range")
  trade_chunk$qty_norm <- normalize(trade_chunk$qty, method="range")
  #plot(x=trade_chunk$date, y=trade_chunk$prices_norm)
  
  end.time <- Sys.time()
  time.taken <- end.time - start.time
  #print (paste(time.taken, "for normalization"))
  
  for (inv in intervals) {
    start.time <- Sys.time()
    
    tmp <- trade_chunk[trade_chunk$date >= (cp-inv) & trade_chunk$date < cp,]
    
    #print (paste(min(tmp$date), "~", max(tmp$date), "(", inv, ")", "has", NROW(tmp), "items"))
    #hist(tmp$prices_norm, main = as.character(inv))
    
    end.time <- Sys.time()
    time.taken <- end.time - start.time
    #print (paste(time.taken, "for temp variables 1"))
    
    start.time <- Sys.time()
    
    #trade volume
    size<-NROW(tmp)
    start_price <- ifelse(size==0,0,tmp$prices_norm[1])
    end_price <- ifelse(size==0,0,tmp$prices_norm[size])
    
    end.time <- Sys.time()
    time.taken <- end.time - start.time
    #print (paste(time.taken, "for temp variables 2"))
    
    start.time <- Sys.time()
    
    
    #x<-table(tmp$type)
    cnt_ask = sum(tmp$type=="ask") #as.integer(x["ask"])
    cnt_bid = sum(tmp$type=="bid") #as.integer(x["bid"])
    
    
    #sum_qty<-sum(tmp$qty)
    
    end.time <- Sys.time()
    time.taken <- end.time - start.time
    #print (paste(time.taken, "for temp variables 3"))
    
    start.time <- Sys.time()
    
    #sum_ask_qty<-sum(tmp[tmp$type=="ask",]$qty)
    #sum_bid_qty<-sum(tmp[tmp$type=="bid",]$qty)
    res<-tapply(tmp$qty, tmp$type=="ask", sum)
    sum_ask_qty<-as.integer(res["TRUE"])
    sum_bid_qty<-as.integer(res["FALSE"])
    sum_qty<-sum_ask_qty+sum_bid_qty#sum(tmp$qty)
    
    end.time <- Sys.time()
    time.taken <- end.time - start.time
    #print (paste(time.taken, "for temp variables 4"))
    
    start.time <- Sys.time()
    
    #add features 
    #NOTE!! when add a new feature, the name of the feature must be added as well 
    train_row = c(train_row, c(size, cnt_ask, cnt_bid, sum_qty, sum_ask_qty, sum_bid_qty, 
                               ifelse(sum_qty==0,0,sum_ask_qty/sum_qty), ifelse(sum_qty==0,0,sum_bid_qty/sum_qty),
                               
                               ifelse(size==0,0,sum_qty/size), ifelse(cnt_ask==0,0,sum_ask_qty/cnt_ask), ifelse(cnt_bid==0,0,sum_bid_qty/cnt_bid), 
                               mean(tmp$prices_norm), sd(tmp$prices_norm), ifelse(size==0,0,start_price), ifelse(size==0,0,end_price), 
                               ifelse(size==0,0,end_price-start_price)))
    
    end.time <- Sys.time()
    time.taken <- end.time - start.time
    #print (paste(time.taken, "for adding features"))
  }
  
  start.time <- Sys.time()

  train[i] <- list(train_row)

  end.time <- Sys.time()
  time.taken <- end.time - start.time
  #print (paste(time.taken, "for appending to a temp train vec"))
  
  progress = as.integer(i*100/total_cnt)
  if (current_progress != progress) {
    current_progress = progress
    print (paste(current_progress, "%"))
    cat("")
  }
  i <- i+1
  
  end.time.sub <- Sys.time()
  time.taken.sub <- end.time.sub - start.time.sub
  #print (paste(time.taken.sub, "for a checkpoint"))
  
}



train <- do.call(rbind,train)


#set column names
cnames = c("checkpoint")
for (inv in intervals) {
  cnames = c( cnames, 
              paste0('size_',inv),
              paste0('cnt_ask_',inv),
              paste0('cnt_bid_',inv),
              paste0('sum_qty_',inv),
              paste0('sum_ask_qty_',inv),
              paste0('sum_bid_qty_',inv),
              paste0('rate_ask_qty_',inv),
              paste0('rate_bid_qty_',inv),
              paste0('avg_qty_',inv),
              paste0('avg_ask_qty_',inv),
              paste0('avg_bid_qty_',inv),
              paste0('mean_price_',inv),
              paste0('sd_price_',inv),
              paste0('start_price_',inv),
              paste0('end_price_',inv),
              paste0('diff_price_',inv)
  )
}
colnames(train) <- cnames


end.time.total <- Sys.time()
time.taken.total <- end.time.total - start.time.total
print (paste(time.taken.total, "for total process"))




##########################################################################################
# create labels
nrow(trades)
