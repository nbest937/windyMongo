library( RJSONIO)
library( RCurl)
library( rmongodb)

meta <- fromJSON( getURL( "https://data.cityofchicago.org/api/views/7nii-7srd.json"))

columnsDf <- do.call(
  rbind,
  lapply(
    meta$columns,
    function( x) {
      data.frame( position= x$position,
                 name= x$name,
                 fieldName= x$fieldName,
                 dataTypeName= x$dataTypeName)}))

colClasses <- rep( "character", length( columnsDf$dataTypeName))
colClasses[ columnsDf$dataTypeName == "checkbox"] <- "logical"
colClasses[ columnsDf$dataTypeName == "number"] <- "numeric"
colClasses[ c( 11, 15, 18, 19, 20)] <- "integer"
  
csv <- file( "data/buildings.csv")
open( csv)

## df <- read.csv( header= FALSE, text=readLines( csv, n=100), stringsAsFactors= FALSE)
## close(csv)

header <- as.list( read.csv( header= FALSE, text=readLines( csv, n=1), stringsAsFactors= FALSE))


data <- as.list( read.csv(
  header= FALSE,
  text=readLines( csv, n=1),
  stringsAsFactors= FALSE,
  colClasses= colClasses))
names( data) <- columnsDf$fieldName
  
buf <- mongo.bson.buffer.create()
mongo.bson.buffer.append(
  buf, "type", "request")
mongo.bson.buffer.append(
  buf, "when",
  strptime( data[[ "date_service_request_was_received"]], format= "%m/%d/%Y"))
mongo.bson.buffer.append(
  buf, "where",
  c( data[[ "longitude"]], data[[ "latitude"]]))
mongo.bson.buffer.append(
  buf, "what",
  mongo.bson.from.list(
    data[ c( "service_request_type", "service_request_number")]))
## mongo.bson.start.object( buf, "what")
## mongo.bson.buffer.append(
##   buf, data[ 1:2])
## mongo.bson.finsih.object( buf)
##mongo.bson.buffer.start.object( buf, "original")
mongo.bson.buffer.append.list( buf, "original", data)
##mongo.bson.buffer.finish.object( buf)

repeat {
  data <- as.list( read.csv( header= FALSE, text=readLines( csv, n=1), stringsAsFactors= FALSE))
  if( all( !is.na(data[ 4:10]))) break
}

n <- 0
repeat {
  t <- readLines( csv, n=1)
  if( length( t) == 0)  break
  n <- n+1
}

library( plyr)

mongo.bson.from.list.with.NAs <- function( data) {
  buf <- mongo.bson.buffer.create()
  l_ply(
    .data= names( data),
    .fun= function( n) {
      if( is.na( data[[ n]])) {
        mongo.bson.buffer.append.null( buf, name= n)
      } else {
        mongo.bson.buffer.append( buf, name= n, value= data[[ n]])}})
  mongo.bson.from.buffer( buf)
  ## buf
}


loadBuildings <- function( fn= "data/buildings.csv", print= TRUE, load= FALSE) {
  csv <- file( fn)
  open( csv)
  header <- as.list(
    read.csv(
      text=readLines( csv, n=1),
      header= FALSE,
      stringsAsFactors= FALSE))
  repeat {
    data <- try(
      as.list(
        read.csv(
          text=readLines( csv, n=1),
          header= FALSE,
          stringsAsFactors= FALSE,
          colClasses= colClasses)),
      silent= TRUE)
    if( inherits(data, "try-error")) break
    names( data) <- columnsDf$fieldName
    buf <- mongo.bson.buffer.create()
    mongo.bson.buffer.append(
      buf, "type", "request")
    mongo.bson.buffer.append(
      buf, "when",
      strptime( data[[ "date_service_request_was_received"]], format= "%m/%d/%Y"))
    mongo.bson.buffer.append(
      buf, "where",
      c( data[[ "longitude"]], data[[ "latitude"]]))
    mongo.bson.buffer.append(
      buf, "what",
      mongo.bson.from.list(
        data[ c( "service_request_type", "service_request_number")]))
    mongo.bson.buffer.append.bson(
      buf, "original",
      mongo.bson.from.list.with.NAs( data))
    bson <- mongo.bson.from.buffer( buf)
    if( print) print( bson)
    if( load) mongo.insert( mongo, ns, bson)
  }
  close( csv)
}

