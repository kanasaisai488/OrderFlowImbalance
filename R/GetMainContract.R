#
# GetMainContract.R:
# 	given the market data for a trading day,
#	determines the main contract Instrument ID either based on Open Interest or Volume


# GetMainContract <- function(data, open.int = F) {
# 	if (open.int == T) {
#         # Read all the data before 9:00
# 		temp <- data[which(data$SecondOfDay < 33240),] # 9:00
#         # Contract with the largest open interest will be the main one
# 		temp <- temp[temp$OpenInterest == max(temp$OpenInterest),]
# 	} else {
#         # Read the data exactly at 9:15 (market opens)
# 		temp <- data[which(data$SecondOfDay == 33300),] # 9:15
#         # Contract with the largest volume will be the main one
# 		temp <- temp[temp$Volume == max(temp$Volume),]
# 	}
#     return(head(temp$InstrumentID, 1))
# }

GetMainContract <- function(data, open.int = FALSE) {
  
  if (open.int == TRUE) {
    # Read all the data before 9:00 (SecondOfDay < 33240)
    temp <- data[which(data$SecondOfDay < 33240),]
    
    if (nrow(temp) == 0 || all(is.na(temp$OpenInterest))) {
      return(NA)  # Fallback if no data
    }
    
    main_value <- max(temp$OpenInterest, na.rm = TRUE)
    temp <- temp[temp$OpenInterest == main_value, ]
    
  } else {
    # Read the data exactly at 9:15 (SecondOfDay == 33300)
    temp <- data[which(data$SecondOfDay == 33300), ]
    
    if (nrow(temp) == 0 || all(is.na(temp$Volume))) {
      return(NA)  # Fallback if no data
    }
    
    main_value <- max(temp$Volume, na.rm = TRUE)
    temp <- temp[temp$Volume == main_value, ]
  }
  
  # Return the first InstrumentID if available
  if ("InstrumentID" %in% colnames(temp) && nrow(temp) > 0) {
    return(head(temp$InstrumentID, 1))
  } else {
    return(NA)
  }
}
