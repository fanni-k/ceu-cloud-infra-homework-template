### Homework_01 - DE4


# Setting Date as the DATE_PARAM variable in your R script
DATE_PARAM="2021-02-12"
date <- as.Date(DATE_PARAM, "%Y-%m-%d")
library(httr)
library(aws.s3)
library(jsonlite)
library(lubridate)

# Retrieving 1000 most viewed Wikipedia articles from API
url <- paste(
  "https://wikimedia.org/api/rest_v1/metrics/pageviews/top/en.wikipedia.org/all-access/",
  format(date, "%Y/%m/%d"), sep='')

wiki.server.response = GET(url)
wiki.response.status = status_code(wiki.server.response)
wiki.response.body = content(wiki.server.response, 'text')

if (wiki.response.status != 200){
  print(paste("Recieved non-OK status code from Wiki Server: ",
              wiki.response.status,
              '. Response body: ',
              wiki.response.body, sep=''
  ))
}

# Creating a local folder and saving the response
RAW_LOCATION_BASE='raw-views'
dir.create(file.path(RAW_LOCATION_BASE), showWarnings = FALSE)


raw.output.filename = paste("raw-views-", format(date, "%Y-%m-%d"), '.txt',
                            sep='')
raw.output.fullpath = paste(RAW_LOCATION_BASE, '/', 
                            raw.output.filename, sep='')
write(wiki.response.body, raw.output.fullpath)

# Uploading the response to S3

keyTable <- read.csv("accessKeys.csv", header = T) 
AWS_ACCESS_KEY_ID <- as.character(keyTable$Access.key.ID)
AWS_SECRET_ACCESS_KEY <- as.character(keyTable$Secret.access.key)
#activate
Sys.setenv("AWS_ACCESS_KEY_ID" = AWS_ACCESS_KEY_ID,
           "AWS_SECRET_ACCESS_KEY" = AWS_SECRET_ACCESS_KEY,
           "AWS_DEFAULT_REGION" = "eu-west-1") 

BUCKET="student-2001405"

put_object(file = raw.output.fullpath,
           object = paste('de4/raw/', 
                          raw.output.filename,
                          sep = ""),
           bucket = BUCKET,
           verbose = TRUE)

# Parse the response and write the parsed string to "Bronze"

# Extracting the most viewed from the server's response
wiki.response.parsed = content(wiki.server.response, 'parsed')
most.viewed = wiki.response.parsed$items[[1]]$articles

# Convert the server's response to JSON lines
current.time = Sys.time() 
json.lines = ""
for (page in most.viewed){
  record = list(
    article = page$article,
    views = page$views,
    rank = page$rank,
    date = format(date, "%Y-%m-%d"),
    retrieved_at = current.time
  )
  
  json.lines = paste(json.lines,
                     toJSON(record,
                            auto_unbox=TRUE),
                     "\n",
                     sep='')
}

# Writing the file on the local computer
JSON_LOCATION_BASE='data-views'
dir.create(file.path(JSON_LOCATION_BASE), showWarnings = FALSE)

json.lines.filename = paste("views-", format(date, "%Y-%m-%d"), '.json',
                            sep='')
json.lines.fullpath = paste(JSON_LOCATION_BASE, '/', 
                            json.lines.filename, sep='')

write(json.lines, file = json.lines.fullpath)

# Uploading the JSON to S3

put_object(file = json.lines.fullpath,
           object = paste('de4/views/', 
                          json.lines.filename,
                          sep = ""),
           bucket = BUCKET,
           verbose = TRUE)

