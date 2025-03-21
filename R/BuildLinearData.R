# BuildLinearData.R:
#	given the market data for a trading session, 
#	builds the variables to be used in a linear model for the trading strategy

#source(file = 'GetMainContract.R')

# BuildLinearData <- function(data,
# 							morning = T, 
# 							open.int = F, 
# 							delay = 20, 
# 							lags = 5,
# 							functions = NULL) {
# 
# 	library(zoo)
# 	library(TTR)
# 
# 	# declare constants
# 	day.start <- 5000#33300 	# 9:15
# 	AM.start <- 6000#33300 	# 9:15
# 	AM.open <- 6100# 33360 	# 9:16 - trade open
# 	AM.close <- 53000#40800 	# 11:20 - trade close
# 	AM.end <- 53200#41280 	# 11:28
# 
# 	PM.start <- 53201#46800 	# 13:00
# 	PM.open <- 53280#46860 	# 13:01 - trade open
# 	PM.close <- 75000#854000 	# 15:00 - trade close
# 	PM.end <-  75500#54780 	# 15:13
# 
# 	start.time <- ifelse(morning, AM.start, PM.start) 	# - data start
# 	open.time <- ifelse(morning, AM.open, PM.open)		# - trade open
# 	close.time <- ifelse(morning, AM.close , PM.close)	# - trade close
# 	end.time <- ifelse(morning, AM.end, PM.end)			# - data end
# 
# 	# get main contract
# 	instrument <- GetMainContract(data, open.int)
# 
# 	# index of all lines between the start time and end time
# 	ind <- which(data$SecondOfDay >= start.time & data$SecondOfDay < end.time)
# 	# filter to have all these lines in 'main.data'
# 	main.data <- data[ind ,]
# 	# the number of lines 
# 	n <- nrow(main.data)
# 
# 	time.secs <- main.data$SecondOfDay + main.data$UpdateMillisec/1000
# 	ind.open <- head(which(time.secs>=open.time),1)
# 	ind.close <- head(which(time.secs>=close.time),1)
# 
# 	# calculate variables
# 	mid.price <- (main.data$BidPrice + main.data$AskPrice)/2
# 	spread <- main.data$AskPrice - main.data$BidPrice
# 
# 	# Order Imbalance Ratio (OIR)
# 	OIR.array <- (main.data$BidVolume - main.data$AskVolume) / (main.data$BidVolume + main.data$AskVolume)
# 	dBid.price <- c(0,diff(main.data$BidPrice))
# 	dAsk.price <- c(0,diff(main.data$AskPrice))
# 
# 	## build order imbalance signal according to Spec
# 	## Volume Order Imbalance (VOI)
# 	## we have found a strong association between VOI and contemporaneous price changes
# 	bid.CV <- (main.data$BidVolume - ifelse(dBid.price==0,c(0,main.data$BidVolume[-n]),rep(0,n)))*as.integer(dBid.price >=0)
# 	ask.CV <- (main.data$AskVolume - ifelse(dAsk.price==0,c(0,main.data$AskVolume[-n]),rep(0,n)))*as.integer(dAsk.price <=0)
# 	VOI.array <- bid.CV - ask.CV
# 
# 	dVol <- c(NA,diff(main.data$Volume))
# 	dTO <- c(NA,diff(main.data$Turnover))
# 	AvgTrade.price <- dTO / dVol / 10000
# 	AvgTrade.price[which(is.nan(AvgTrade.price))] <- NA
# 	AvgTrade.price <- na.locf(na.locf(AvgTrade.price , na.rm=F), fromLast=T)
# 	# mid-price basis (MPB), 
# 	# is an important predictor of price change. 
# 	# It gives a continuous classiffication of 
# 	# whether trades were buyer or seller initiated. 
# 	# A large positive (negative) quantity means 
# 	# the trades were, on average, closer to the ask (bid) price.
# 	MPB.array <- (AvgTrade.price - c(mid.price[1], rollmean(mid.price, k=2)))
# 
# 	k <- delay
# 	p <- lags
# 	new.ind <- (p+1):(n-k)
# 
# 	## arithmetic average of future k midprices minus current midprice
# 	if (k > 0 && n > k) {
# 	  fpc <- rollmean(mid.price, k=k)[-1] - mid.price[1:(n-k)]
# 	  #dMid.Response <- c(fpc, rep(NA, k))
# 	  dMid.Response[1:length(fpc)] <- fpc
# 	} else {
# 	  dMid.Response <- rep(NA, n)
# 	}
# 	# if (k > 0) {
# 	# 
# 	# 	library(zoo)
# 	# 	fpc <- rollmean(mid.price, k=k)[-1] - mid.price[1:(n-k)]
# 	# 	dMid.Response <- c(fpc, rep(NA,k))
# 	# 
# 	# } else {
# 	# 	dMid.Response <- rep(0,n)
# 	# }
# 
# 	# build VOI, dMid, OIR - first p entries are useless
# 	VOI <- cbind(VOI.array)
# 	OIR <- cbind(OIR.array)
# 	MPB <- cbind(MPB.array)
# 
# 	if (p > 0) {
# 
# 		for (j in 1:p) {
# 
# 			VOI <- cbind(VOI, c(rep(NA,j), VOI.array[1:(n-j)]))
# 			OIR <- cbind(OIR, c(rep(NA,j), OIR.array[1:(n-j)]))
# 			MPB <- cbind(MPB, c(rep(NA,j), MPB.array[1:(n-j)]))
# 		}
# 	}
# 
# 	# trim the variables
# 	dMid.Response <- dMid.Response[new.ind]
# 	VOI <- VOI[new.ind,,drop=FALSE]
# 	OIR <- OIR[new.ind,,drop=FALSE]
# 	MPB <- MPB[new.ind,,drop=FALSE]
# 
# 	colnames(VOI) <- paste('VOI.t',seq(0,p),sep='')
# 	colnames(OIR) <- paste('OIR.t',seq(0,p),sep='')
# 	colnames(MPB) <- paste('MPB.t',seq(0,p),sep='')
# 
# 	# trim the other supporting data
# 	mid.price <- mid.price[new.ind]
# 	spread <- spread[new.ind]
# 	AvgTrade.price <- AvgTrade.price[new.ind]
# 	main.data <- main.data[new.ind ,]
# 	time.secs <- time.secs[new.ind]
# 
# 	ind.open <- ind.open - p
# 	ind.close <- ind.close - p
# 
# 	# return an R object
# 	value <- {}
# 	value$data <- main.data
# 	value$dMid.Response <- dMid.Response
# 	value$VOI <- VOI
# 	value$OIR <- OIR
# 	value$MPB <- MPB
# 
# 	value$time.secs <- time.secs
# 	value$ind.open <- ind.open
# 	value$ind.close <- ind.close
# 
# 	value$mid.price <- mid.price
# 	spread[which(spread ==0)] <- TICK.PRICE
# 	value$spread <- spread
# 	value$AvgTrade.price <- AvgTrade.price
# 
# 	return(value)
# }

# BuildLinearData.R:
# Builds variables to be used in a linear model for the trading strategy

BuildLinearData <- function(data,
                            morning = TRUE, 
                            open.int = FALSE, 
                            delay = 20, 
                            lags = 5,
                            functions = NULL) {
  
  library(zoo)
  library(TTR)
  
  # Default tick price value if spread = 0
  TICK.PRICE <- 1e-5
  
  # Time constants
  AM.start <- 6000
  AM.open  <- 6100
  AM.close <- 53000
  AM.end   <- 53200
  
  PM.start <- 53201
  PM.open  <- 53280
  PM.close <- 75000
  PM.end   <- 75500
  
  # Session timing
  start.time <- ifelse(morning, AM.start, PM.start)
  open.time  <- ifelse(morning, AM.open, PM.open)
  close.time <- ifelse(morning, AM.close, PM.close)
  end.time   <- ifelse(morning, AM.end, PM.end)
  
  # Get main contract (optional logic)
  instrument <- GetMainContract(data, open.int)
  
  # Filter session data
  ind <- which(data$SecondOfDay >= start.time & data$SecondOfDay < end.time)
  main.data <- data[ind, ]
  n <- nrow(main.data)
  
  # Exit early if no data
  if (n == 0) {
    warning("No data available in selected session window.")
    return(NULL)
  }
  
  # Calculate time, open/close index
  time.secs <- main.data$SecondOfDay + main.data$UpdateMillisec / 1000
  ind.open  <- head(which(time.secs >= open.time), 1)
  ind.close <- head(which(time.secs >= close.time), 1)
  
  # Core calculations
  mid.price <- (main.data$BidPrice + main.data$AskPrice) / 2
  spread <- main.data$AskPrice - main.data$BidPrice
  spread[spread == 0] <- TICK.PRICE
  
  # OIR (Order Imbalance Ratio)
  OIR.array <- (main.data$BidVolume - main.data$AskVolume) / 
    (main.data$BidVolume + main.data$AskVolume)
  
  dBid.price <- c(0, diff(main.data$BidPrice))
  dAsk.price <- c(0, diff(main.data$AskPrice))
  
  # VOI (Volume Order Imbalance)
  bid.CV <- (main.data$BidVolume - 
               ifelse(dBid.price == 0, c(0, main.data$BidVolume[-n]), rep(0, n))) *
    as.integer(dBid.price >= 0)
  
  ask.CV <- (main.data$AskVolume - 
               ifelse(dAsk.price == 0, c(0, main.data$AskVolume[-n]), rep(0, n))) *
    as.integer(dAsk.price <= 0)
  
  VOI.array <- bid.CV - ask.CV
  
  # Average trade price
  dVol <- c(NA, diff(main.data$Volume))
  dTO  <- c(NA, diff(main.data$Turnover))
  AvgTrade.price <- dTO / dVol / 10000
  AvgTrade.price[is.nan(AvgTrade.price)] <- NA
  AvgTrade.price <- na.locf(na.locf(AvgTrade.price, na.rm = FALSE), fromLast = TRUE)
  
  # Mid-price basis (MPB)
  MPB.array <- (AvgTrade.price - c(mid.price[1], rollmean(mid.price, k = 2)))
  
  k <- delay
  p <- lags
  
  # Initialize dMid.Response safely
  dMid.Response <- rep(NA, n)
  
  if (k > 0 && n > k) {
    roll_avg <- rollmean(mid.price, k = k)
    idx <- seq_len(n - k)
    fpc <- roll_avg[-1]
    
    # Adjust for length mismatch safety
    min_len <- min(length(idx), length(fpc))
    if (min_len > 0) {
      dMid.Response[idx[1:min_len]] <- fpc[1:min_len] - mid.price[idx[1:min_len]]
    }
  }
  
  # Build VOI, OIR, MPB matrices with lags
  VOI <- cbind(VOI.array)
  OIR <- cbind(OIR.array)
  MPB <- cbind(MPB.array)
  
  if (p > 0 && n > p) {
    for (j in 1:p) {
      VOI <- cbind(VOI, c(rep(NA, j), VOI.array[1:(n - j)]))
      OIR <- cbind(OIR, c(rep(NA, j), OIR.array[1:(n - j)]))
      MPB <- cbind(MPB, c(rep(NA, j), MPB.array[1:(n - j)]))
    }
  }
  
  # Final index after trimming for lags/delays
  new.ind <- (p + 1):(n - k)
  if (length(new.ind) == 0 || min(new.ind) > n || max(new.ind) > n) {
    warning("Not enough data points after applying lags/delay.")
    return(NULL)
  }
  
  # Trim all data to final usable range
  dMid.Response  <- dMid.Response[new.ind]
  VOI            <- VOI[new.ind, , drop = FALSE]
  OIR            <- OIR[new.ind, , drop = FALSE]
  MPB            <- MPB[new.ind, , drop = FALSE]
  colnames(VOI)  <- paste0("VOI.t", seq(0, p))
  colnames(OIR)  <- paste0("OIR.t", seq(0, p))
  colnames(MPB)  <- paste0("MPB.t", seq(0, p))
  
  mid.price      <- mid.price[new.ind]
  spread         <- spread[new.ind]
  AvgTrade.price <- AvgTrade.price[new.ind]
  main.data      <- main.data[new.ind, ]
  time.secs      <- time.secs[new.ind]
  
  ind.open  <- ind.open - p
  ind.close <- ind.close - p
  
  # Return final structured result
  value <- list(
    data = main.data,
    dMid.Response = dMid.Response,
    VOI = VOI,
    OIR = OIR,
    MPB = MPB,
    time.secs = time.secs,
    ind.open = ind.open,
    ind.close = ind.close,
    mid.price = mid.price,
    spread = spread,
    AvgTrade.price = AvgTrade.price
  )
  
  return(value)
}

