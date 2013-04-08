library( RJSONIO)
library( RCurl)
library( rmongodb)
library( plyr)

dataSets <- list( "buildings", "crimes")
names( dataSets) <- dataSets

fourByFours <- list(
  buildings= "7nii-7srd",
  crimes= "ijzp-q8t2")

integerFields <- list(
  buildings= c(
    "address_street_number",
    "zip_code",
    "ward",
    "police_district",
    "community_area"),
  crimes= c(
    "id",
    "ward",
    "year",
    "x_coordinate",
    "y_coordinate"))

columnsFunc <-
  function( meta) {
    do.call(
      rbind,
      lapply(
        meta$columns,
        function( x) {
          data.frame(
            position= x$position,
            name= x$name,
            fieldName= x$fieldName,
            dataTypeName= x$dataTypeName)}))}

colClassesFunc <-
  function( dataSetName,
           fourByFour= fourByFours[[ dataSetName]],
           integerFieldsVec= integerFields[[ dataSetName]]) {
    meta <- fromJSON( getURL( sprintf(
      "https://data.cityofchicago.org/api/views/%s.json",
      fourByFour)))
    columnsDf <- columnsFunc( meta)
    colClasses <- rep( "character", length( columnsDf$dataTypeName))
    names( colClasses) <- columnsDf$fieldName
    colClasses[ columnsDf$dataTypeName == "checkbox"] <- "logical"
    colClasses[ columnsDf$dataTypeName == "number"] <- "numeric"
    colClasses[ integerFieldsVec] <- "integer"
    colClasses
  }

colClasses <-
  lapply(
    dataSets,
    colClassesFunc)


## ## this code is useful for testing the line-by-line approach to
## ## reading in the data
  
## csv <- file( "data/crimes.csv")

## open( csv)

## header <- as.list( read.csv( header= FALSE, text=readLines( csv, n=1), stringsAsFactors= FALSE))
## data <- as.list( read.csv( header= FALSE, text=readLines( csv, n=1), stringsAsFactors= FALSE))

## ## scan the data for a record that has more non-null fields
  
## repeat {
##   data <- as.list( read.csv( header= FALSE, text=readLines( csv, n=1), stringsAsFactors= FALSE))
##   if( all( !is.na(data[ 4:10]))) break
## }

## n <- 0
## repeat {
##   t <- readLines( csv, n=1)
##   if( length( t) == 0)  break
##   n <- n+1
## }

## close( csv)



## If we want to include null values in Mongo we need to use a
## function like this.  Currently this is not used.  Null fields in
## input are simply omitted.

## mongo.bson.from.list.with.NAs <- function( data) {
##   buf <- mongo.bson.buffer.create()
##   l_ply(
##     .data= names( data),
##     .fun= function( n) {
##       if( is.na( data[[ n]])) {
##         mongo.bson.buffer.append.null( buf, name= n)
##       } else {
##         mongo.bson.buffer.append( buf, name= n, value= data[[ n]])}})
##   mongo.bson.from.buffer( buf)
##   ## buf
## }

recordExpressions <-
  list(
    buildings= quote(
      list(
        type= "request",
        when= strptime( date_service_request_was_received, format= "%m/%d/%Y"),
        where= c( longitude, latitude),
        what= list(
          service_request_type= service_request_type,
          service_request_number= service_request_number),
        original= data[ !is.na( data)])),
    crimes= quote(
      list(
        type= "event",
        when= strptime( date, format= "%m/%d/%Y"),
        where= c( longitude, latitude),
        what= list(
          primary_type= primary_type,
          description= description,
          location_description= location_description),
        who= beat,
        original= data[ !is.na(data)])))

## mongo.bson.from.list(
##   with(
##     data,
##     eval( recordExpressions$buildings)))

loadData <-
  function( dataSet= "buildings",
           fileName= sprintf( "data/%s.csv", dataSet),
           nameSpace= sprintf( "nbest.%s", dataSet),
           ## colClasses= colClasses[[ dataSet]], # causes bug in try()?
           schema= recordExpressions[[ dataSet]],
           print= TRUE,
           mongo= NA) {
    csv <- file( fileName)
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
            colClasses= colClasses[[ dataSet]])),
        silent= TRUE)
      if( inherits(data, "try-error")) break ##recover()
      names( data) <- names( colClasses[[ dataSet]])
      bson <-
        mongo.bson.from.list(
          with(
            data,
            eval( schema)))
      if( print) print( bson)
      if( !is.na( mongo)) mongo.insert( mongo, nameSpace, bson)
    }
    close( csv)
  }

mongo <- mongo.create( db= "nbest")
mongo.drop( mongo, "nbest.windy")
loadData( dataSet= "buildings", nameSpace= "nbest.windy", print= FALSE, mongo= mongo)
loadData( dataSet= "crimes", nameSpace= "nbest.windy", print= FALSE, mongo= mongo)
