# process_GHCN.R
# Version 1.5.1
# Project: TMax
#
# By Xiaojing Tang
# Created On: 11/22/2014
# Last Update: 12/28/2016
#
# Version 1.0 - 11/12/2014
#   This script is for processing GHCN-D data.
#   Step 1: Extract daily temperature data from full dataset.
#
# Updates of Version 1.1 - 12/2/2014
#   1.Bugs fixed.
#   2.Added step 2: split data by stations.
#
# Updates of Version 1.2 - 1/3/2015
#   1.Added step 3: combine data files of same station.
#   2.Added step 4: calculate max and min year for each day of year.
#
# Updates of Version 1.3 - 1/31/2015
#   1.Added step 5: calculate TMax.
#   2.Added step 5.1: extract max and min year for each day of year.
#
# Updates of Version 1.4 - 2/28/2015
#   1.Added stage 5.2 processing.
#
# Updates of Version 1.4.1 - 3/2/2015
#   1.Bug fixed.
#
# Updates of Version 1.4.2 - 3/6/2015
#   1.Removed leap year.
#   2.Extract the shortest number of record.
#
# Updates of Version 1.5 - 7/28/2015
#   1.Added stage 5.3 processing.
#
# Updates of Version 1.5.1 - 12/28/2016
#   1.Clean up.
#   2.Add comments.
#   3.Make it more readable
#
# ----------------------------------------------------------------

# library and sourcing
library(RCurl)
script <- getURL('https://raw.githubusercontent.com/xjtang/rTools/master/source_all.R',ssl.verifypeer=F)
eval(parse(text=script),envir=.GlobalEnv)
#
# ----------------------------------------------------------------

# main functions

# filter_GHCN (step 1)
# Filter GHCN-D dataset to get daily temperature data only
#
# Input Arguments:
#   path (String) - full path to raw GHCN data files
#   output (String) - full path of output location
#   i - identify the data file to be processed by this job
#
filter_GHCN <- function(path,output,i){

  # get list of available data files
  fileList <- list.files(path,full.name=T,recursive=T)

  # read current file
  raw <- read.table(fileList[i],header=T,sep=',',quote='"',colClasses=c('character','character','character','numeric','character','character','character','character'),stringsAsFactors=F)

  # grab temperature data
  tmax <- raw[raw[,3]=='TMAX',]
  tmin <- raw[raw[,3]=='TMIN',]

  # grab current year
  year <- strLeft(raw[1,2],4)

  # write output
  write.table(tmax,file=paste(output,year,'_max.csv',sep=''),sep=',',row.names=F,col.names=F)
  write.table(tmin,file=paste(output,year,'_min.csv',sep=''),sep=',',row.names=F,col.names=F)

  # done
  return(0)

}
# ----------------------------------------

# station_GHCN (step 2)
# Split the data by station
#
# Input Arguments:
#   path (String) - full path to results from step 1
#   output (String) - full path of output location
#   tmp (String) - either max or min, which to process
#   i - identify the data file to be processed by this job
#
station_GHCN <- function(path,output,tmp='max',i){

  # get file list
  pat <- paste('*',tmp,'*.csv',sep='')
  fileList <- list.files(path,pattern=pat,full.name=T,recursive=T)

  # read current file
  raw <- read.table(fileList[i],header=F,sep=',',quote='"',colClasses=c('character','character','character','numeric','character','character','character','character'),stringsAsFactors=F)

  # grab current year
  year <- strLeft(raw[1,2],4)

  # get list of station ids
  sidList <- unique(raw[,1])

  # loop through all station
  for(j in 1:length(sidList)){

    # grab data of current station only
    data <- raw[raw[,1]==sidList[j],]

    # check if output folder for current station already exist
    outDir <- paste(output,sidList[j],sep='')
    if(!file.exists(outDir)){
      dir.create(outDir)
    }

    # add output files to the output folder for current station
    outFile <- paste(outDir,'/',sidList[j],'_',year,'_',tmp,'.csv',sep='')
    write.table(data,file=outFile,append=T,sep=',',row.names=F,col.names=F)

  }

  # done
  return(0)

}
# ----------------------------------------

# combine_GHCN (step 3)
# Combine data files of each station into one file
# Input Arguments:
#   path (String) - full path to results from step 2
#   file (String) - full path of a file of a list of all station ids
#   output (String) - full path of output location
#   tmp (String) - either max or min, which to process
#   i - ith job in the batch processing
#
combine_GHCN <- function(path,file,output,tmp='max',i){

  # get station list
  sidList <- read.table(file,stringsAsFactors=F)[,1]

  # find out the stations to be process by this job
  piece <- 30
  total <- length(sidList)
  n <- round(total/piece)
  segStart <- 1+piece*(i-1)
  if(i<n){
    segEnd <- segStart+piece-1
  }else{
    segEnd <- total
  }

  # subset station id list to those to be processed by this job
  sidList <- sidList[segStart:segEnd]

  # loop through all stations
  for(j in 1:length(sidList)){

    # get list of data files of this station
    cDir <- paste(path,tmp,'/',sidList[j],'/',sep='')
    fileList <- list.files(cDir,'.*csv',full.names=T)

    # loop through file list
    for(k in 1:length(fileList)){

      # initiate result
      record <- rep(-9999,367)

      # read current data file
      raw <- read.table(fileList[k],header=F,sep=',',quote='"',colClasses=c('character','character','character','numeric','character','character','character','character'),stringsAsFactors=F)

      # check if qualified data exist
      raw <- raw[raw[,6]=='',]
      if(nrow(raw)==0){
        next
      }

      # grab temperature data
      year <- as.integer(strLeft(raw[1,2],4))
      record[1] <- year
      dMax <- dateToDOY(year,12,31,dayOnly=T)
      for(l in 1:nrow(raw)){
        month <- as.integer(substr(raw[l,2],5,6))
        day <- as.integer(substr(raw[l,2],7,8))
        doy <- dateToDOY(year,month,day,dayOnly=T)
        record[doy+1] <- raw[l,4]
      }

      # deal with leap year
      if(dMax==366){
        leap <- record[61]
        record[61:366] <- record[62:367]
        record[367] <- leap
      }

      # append record to output file
      outFile <- paste(output,sidList[j],'_',tmp,'.csv',sep='')
      write.table(t(record),outFile,append=T,sep=',',row.names=F,col.names=F)

    }

    # print out progress
    showProgress(j,length(sidList),5)

  }

  # done
  return(0)

}
# ----------------------------------------

# process_GHCN (step 4)
# Calculate max or min year of each day of year for each station
# Input Arguments:
#   path (String) - full path to results from step 3
#   file (String) - full path of a file of a list of all station ids
#   output (String) - full path of output location
#   i - ith job in the batch processing
#
process_GHCN <- function(path,file,output,i){

  # get station list
  sidList <- read.table(file,stringsAsFactors=F)[,1]

  # find out the stations to be process by this job
  piece <- 100
  total <- length(sidList)
  n <- round(total/piece)
  segStart <- 1+piece*(i-1)
  if(i<n){
    segEnd <- segStart+piece-1
  }else{
    segEnd <- total
  }

  # subset station id list to those to be processed by this job
  sidList <- sidList[segStart:segEnd]

  # lop through all stations
  for(j in 1:length(sidList)){

    # forge input file names
    maxFile <- paste(path,'/max/',sidList[j],'_','max','.csv',sep='')
    minFile <- paste(path,'/min/',sidList[j],'_','min','.csv',sep='')
    if(!file.exists(maxFile)){next}
    if(!file.exists(minFile)){next}

    # read input file
    maxRaw <- read.table(maxFile,header=F,sep=',',quote='"')
    minRaw <- read.table(minFile,header=F,sep=',',quote='"')

    # initiate result
    record <- matrix(-1,366,10)

    # calculate results for each day of year
    for(k in 1:366){

      # get data for this day
      maxDay <- maxRaw[,k+1]
      minDay <- minRaw[,k+1]

      # see if there's valid data
      if(length(maxDay[maxDay!=-9999])==0){next}
      if(length(minDay[minDay!=-9999])==0){next}

      # find out which year max and min occured
      Temp <- maxDay
      Temp[Temp==-9999] <- NA
      maxRec <- which.max(Temp)
      Temp <- minDay
      Temp[Temp==-9999] <- NA
      minRec <- which.min(Temp)

      # year of recent high
      record[k,1] <- maxRaw[maxRec,1]
      # total number of valid observations
      record[k,2] <- length(maxDay[maxDay!=-9999])
      # start year
      record[k,3] <- maxRaw[1,1]
      # total number of all records
      record[k,4] <- maxRaw[nrow(maxRaw),1]
      # year of recent low
      record[k,5] <- minRaw[minRec,1]
      # total number of valid observation
      record[k,6] <- length(minDay[minDay!=-9999])
      # start year
      record[k,7] <- minRaw[1,1]
      # total number of all records
      record[k,8] <- minRaw[nrow(minRaw),1]

      # is it warming or cooling?
      if(record[k,1]>record[k,5]){
        # warming (max is more recent)
        record[k,9] <- 1
        record[k,10] <- 0
      }else if(record[k,1]<record[k,5]){
        # cooling (min is more recent)
        record[k,9] <- 0
        record[k,10] <- 1
      }else{
        # tie, same year
        record[k,9] <- 1
        record[k,10] <- 1
      }

    }

    # append record
    outFile <- paste(output,sidList[j],'_','result','.csv',sep='')
    write.table(record,outFile,append=F,sep=',',row.names=F,col.names=F)

    # show progress
    showProgress(j,length(sidList),5)

  }

  # done
  return(0)

}
# ----------------------------------------

# tmax_GHCN (step 5)
# Calculate TMax
# Input Arguments:
#   path (String) - full path to results from step 4
#   file (String) - full path of a file of a list of all station ids
#   output (String) - full path of output location
#
tmax_GHCN <- function(path,file,output){

  # get station list
  sidList <- read.table(file,stringsAsFactors=F)[,1]

  # loop through all stations
  for(i in 1:length(sidList)){

    # check if input file for this station exist
    inFile <- paste(path,'/',sidList[i],'_','result','.csv',sep='')
    if(!file.exists(inFile)){next}

    # read input file for this station
    raw <- read.table(inFile,header=F,sep=',',quote='"')

    # remove leap year
    raw <- raw[1:365,]

    # initiate result
    sid <- sidList[i]
    warm <- 0
    cold <- 0
    tie <- 0
    miss <- 0
    recLength <- 0

    # calculate results
    miss <- sum((raw[,9]==-1)|raw[,10]==-1)
    tie <- sum((raw[,9]==1)&raw[,10]==1)
    warm <- sum((raw[,9]==1)&raw[,10]==0)
    cold <- sum((raw[,9]==0)&raw[,10]==1)

    # check if result add up to 365
    if(warm+cold+tie+miss!=365){
      cat('Error!\n')
      next
    }

    # calculate average length of record
    recLength <- round((sum(raw[,2])+sum(raw[,6]))/2/365)

    # put together result for this station
    record <- data.frame(sid,warm,cold,tie,miss,recLength)

    # append result to output file
    outFile <- paste(output,'/','result1','.csv',sep='')
    write.table(record,outFile,append=T,sep=',',row.names=F,col.names=F)

    # show progress
    showProgress(i,length(sidList),5)

  }

  # done
  return(0)

}
# ----------------------------------------

# year_GHCN (step 5.1)
# Extract max and min year of each day of year for all stations
# Input Arguments:
#   path (String) - full path to results from step 4
#   file (String) - full path of a file of a list of all station ids
#   output (String) - full path of output location
#
compose2_GHCN_D <- function(path,file,output){

  # get station list
  sidList <- read.table(file,stringsAsFactors=F)[,1]

  # loop through all stations
  for(i in 1:length(sidList)){

    # check if input file for this station exist
    inFile <- paste(path,'/',sidList[i],'_','result','.csv',sep='')
    if(!file.exists(inFile)){next}

    # read input file for this station
    raw <- read.table(inFile,header=F,sep=',',quote='"')

    # remove leap year
    raw <- raw[1:365,]

    # initiate result
    sid <- sidList[i]
    result <- matrix(0,1,365*2)

    # grab min and mix year of each day of year for this station
    result[1:365] <- t(raw[,1])
    result[366:(365*2)] <- t(raw[,5])

    # put together result for this station
    record <- data.frame(sid,result)

    # append result to output file
    outFile <- paste(output,'/','result2','.csv',sep='')
    write.table(record,outFile,append=T,sep=',',row.names=F,col.names=F)

    # show progress
    showProgress(i,length(sidList),5)

  }

  # done
  return(0)

}
# ----------------------------------------

# length_GHCN (step 5.2)
# Get record lenth and consistency information for each station
# Input Arguments:
#   path (String) - full path to results from step 4
#   file (String) - full path of a file of a list of all station ids
#   output (String) - full path of output location
#
length_GHCN <- function(path,file,output){

  # get station list
  sidList <- read.table(file,stringsAsFactors=F)[,1]

  # loop through all stations
  for(i in 1:length(sidList)){

    # check if input file exist
    inFile <- paste(path,'/',sidList[i],'_','result','.csv',sep='')
    if(!file.exists(inFile)){next}

    # read input data file
    raw <- read.table(inFile,header=F,sep=',',quote='"')
    raw <- raw[1:365,]
    raw[raw[]==-1] <- NA

    # initiate result
    sid <- sidList[i]
    start <- 0
    end <- 0
    nrec <- 0
    nmiss <- 0
    nmin <- 0

    # calculate results
    # starty year
    start <- min(c(raw[,3],raw[,7]),na.rm=T)
    # end year
    end <- max(c(raw[,4],raw[,8]),na.rm=T)
    # number of valid observation
    nrec <- max(c(raw[,2],raw[,6]),na.rm=T)
    # number of missing observations
    nmiss <- end-start+1-nrec
    # minimum number of observations
    nmin <- min(c(raw[,2],raw[,6]),na.rm=T)

    # put together result for this station
    record <- data.frame(sid,start,end,nrec,nmiss,nmin)

    # append result to output file
    outFile <- paste(output,'/','result3','.csv',sep='')
    write.table(record,outFile,append=T,sep=',',row.names=F,col.names=F)

    # show progress
    showProgress(i,length(sidList),5)

  }

  # done
  return(0)

}
# ----------------------------------------

# recent_GHCN (step 5.3)
# Calculate TMax for in recent years
# Input Arguments:
#   path (String) - full path to results from step 4
#   file (String) - full path of a file of a list of all station ids
#   output (String) - full path of output location
#   yearThres (Integer) - year threhold for recency
#
recent_GHCN <- function(path,file,output,yearThres){

  # get station list
  sidList <- read.table(file,stringsAsFactors=F)[,1]

  # loop through all stations
  for(i in 1:length(sidList)){

    # check if input file exist
    inFile <- paste(path,'/',sidList[i],'_','result','.csv',sep='')
    if(!file.exists(inFile)){next}

    # read input file for this station
    raw <- read.table(inFile,header=F,sep=',',quote='"')

    # remove leap year result
    raw <- raw[1:365,]

    # initiate result
    sid <- sidList[i]
    tmax <- 0
    tmin <- 0

    # calculate TMax in recent years
    tmax <- sum((raw[,9]==1)&raw[,10]==0&(raw[,1]>=yearThres))
    tmin <- sum((raw[,9]==0)&raw[,10]==1&(raw[,5]>=yearThres))

    # put together result for this station
    record <- data.frame(sid,tmax,tmin)

    # append result to output file
    outFile <- paste(output,'/','result4','.csv',sep='')
    write.table(record,outFile,append=T,sep=',',row.names=F,col.names=F)

    # show progress
    showProgress(i,length(sidList),5)

  }

  # done
  return(0)

}
