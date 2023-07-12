#Name: Bingqian Yang
#Course: CS5200 Spr 2023
#Date: 4/16/2023


#Part 1
library(XML)
library(RSQLite)
library(dplyr)
library(sqldf)


#Create a normalized relational schema that contains the 
#following entities/tables: Articles, Journals, Authors.

# Connect to the SQLite database file
dbcon <- dbConnect(RSQLite::SQLite(), "part1.db")

# Create the Authors table
dbExecute(dbcon, "DROP TABLE IF EXISTS Authors")
dbExecute(dbcon, "CREATE TABLE Authors (
                  AuthorID INTEGER PRIMARY KEY,
                  LastName VARCHAR(255),
                  ForeName VARCHAR(255),
                  Initials VARCHAR(20),
                  Affiliation VARCHAR(255)
              )")



# Create the Journals table
dbExecute(dbcon, "DROP TABLE IF EXISTS Journals")
dbExecute(dbcon, "CREATE TABLE Journals (
                  JournalID INTEGER PRIMARY KEY,
                  ISSN VARCHAR(255),
                  CitedMedium VARCHAR(20),
                  Volume INTEGER,
                  Issue INTEGER,
                  PubDate TEXT,
                  Title VARCHAR(255),
                  ISOAbbreviation VARCHAR(255)
              )")



# Create the Articles table
dbExecute(dbcon, "DROP TABLE IF EXISTS Articles")
dbExecute(dbcon, "CREATE TABLE Articles (
                  PMID INTEGER PRIMARY KEY,
                  JournalID INTEGER,
                  ArticleTitle VARCHAR(255),
                  FOREIGN KEY (JournalID) REFERENCES Journals(JournalID)
              )")

# Create the ArticleAuthors table
dbExecute(dbcon, "DROP TABLE IF EXISTS Authorships")
dbExecute(dbcon, "CREATE TABLE Authorships (
                  PMID INTEGER,
                  AuthorID INTEGER,
                  PRIMARY KEY (PMID, AuthorID),
                  FOREIGN KEY (PMID) REFERENCES Articles(PMID),
                  FOREIGN KEY (AuthorID) REFERENCES Authors(AuthorID)
              )")


# Load and parse XML file
#xmlFile <- "pubmed-tfm-xml/subset1.xml"
xmlFile <- "pubmed-tfm-xml/pubmed22n0001-tf.xml"
xmlDOM <- xmlParse(xmlFile)
#root <- xmlRoot(xmlDOM)
#check the number of articles in the file
#numArticles <- xmlSize(root)


# Initialize empty data frames for each table
Author.df <- data.frame(AuthorID = integer(),
                         LastName = character(),
                         ForeName = character(),
                         Initials = character(),
                         Affiliation = character(),
                         stringsAsFactors = FALSE)


Journal.df <- data.frame(JournalID = integer(),
                         ISSN = character(),
                         CitedMedium = character(),
                         Volume = integer(),
                         Issue = integer(),
                         PubDate = as.Date(character(), "%Y-%m-%d"),
                         Title = character(),
                         ISOAbbreviation = character(),
                         stringsAsFactors = FALSE)

Article.df <- data.frame(PMID = integer(),
                         JournalID = integer(),
                         ArticleTitle = character(),
                         stringsAsFactors = FALSE)


Authorship.df <- data.frame(PMID = integer(),
                            AuthorID = integer())





# Define a named vector to map month abbreviations to numbers
month_map <- c("Jan" = "01", "Feb" = "02", "Mar" = "03", "Apr" = "04", "May" = "05", "Jun" = "06", 
               "Jul" = "07", "Aug" = "08", "Sep" = "09", "Oct" = "10", "Nov" = "11", "Dec" = "12")



#Extract Journal Information
PMIDs <- xpathSApply(xmlDOM,"//Article",xmlGetAttr, 'PMID')

for (i in 1:length(PMIDs)) {
  curNode = getNodeSet(xmlDOM, paste0("//Article[@PMID=", PMIDs[i], "]"))
  Journal.df[i,1] <- i
  Journal.df[i,2] <- xmlValue(curNode[[1]][["PubDetails"]][["Journal"]][["ISSN"]])
  Journal.df[i,3] <- xmlGetAttr(curNode[[1]][["PubDetails"]][["Journal"]][["JournalIssue"]], "CitedMedium")
  Journal.df[i,4] <- xmlValue(curNode[[1]][["PubDetails"]][["Journal"]][["JournalIssue"]][["Volume"]])
  Journal.df[i,5] <- xmlValue(curNode[[1]][["PubDetails"]][["Journal"]][["JournalIssue"]][["Issue"]])


  year = xmlValue(curNode[[1]][["PubDetails"]][["Journal"]][["JournalIssue"]][["PubDate"]][["Year"]])
  month_abbrev <- xmlValue(curNode[[1]][["PubDetails"]][["Journal"]][["JournalIssue"]][["PubDate"]][["Month"]])
  month <- as.character(month_map[month_abbrev])
  day = xmlValue(curNode[[1]][["PubDetails"]][["Journal"]][["JournalIssue"]][["PubDate"]][["Day"]])
  #set the default date if it is missing or in other format
  if (is.na(year)) {
    year = "2055"
    month = "01"
  } 
  if (is.na(month)) {
    month = "01"
  }
  if (is.na(day)) {
    day = "01"
  }
  pubdate <- as.Date(paste0(year, month, day), "%Y%m%d")
  Journal.df[i,6] <- pubdate
  Journal.df[i,7] <- xmlValue(curNode[[1]][["PubDetails"]][["Journal"]][["Title"]])
  Journal.df[i,8] <- xmlValue(curNode[[1]][["PubDetails"]][["Journal"]][["ISOAbbreviation"]])
  
}
#remove duplicate journals
Journal.df <- Journal.df %>% distinct(ISSN, Title, Volume, Issue, PubDate, .keep_all = TRUE)
#Journal.df




#Extract author information
authIdx = 1
for (i in 1:length(PMIDs)) {
  curNode = getNodeSet(xmlDOM, paste0("//Article[@PMID=", PMIDs[i], "]"))
  idx = 1
  while (!is.null(curNode[[1]][["PubDetails"]][["AuthorList"]][[idx]])) {
    foreName = xmlValue(curNode[[1]][["PubDetails"]][["AuthorList"]][[idx]][["ForeName"]])
    lastName = xmlValue(curNode[[1]][["PubDetails"]][["AuthorList"]][[idx]][["LastName"]])
    initials = xmlValue(curNode[[1]][["PubDetails"]][["AuthorList"]][[idx]][["Initials"]])
    affiliation = xmlValue(curNode[[1]][["PubDetails"]][["AuthorList"]][[idx]][["AffiliationInfo"]][["Affiliation"]])
    if (is.na(foreName)) {
      foreName = ""
    }
    if (is.na(lastName)) {
      lastName = ""
    }
    if (is.na(initials)) {
      initials = ""
    }
    if (is.na(affiliation)) {
      affiliation = ""
    }
    Author.df[authIdx, 1] = authIdx
    Author.df[authIdx, 2] = lastName
    Author.df[authIdx, 3] = foreName
    Author.df[authIdx, 4] = initials
    Author.df[authIdx, 5] = affiliation
    authIdx = authIdx + 1
    idx = idx + 1
  }
  
}
#remove duplicate authors
Author.df <- Author.df %>% distinct(LastName, ForeName, .keep_all = TRUE)
#Author.df



# Extract Article 
index = 1
for (i in 1:length(PMIDs)) {
  
  curNode <- getNodeSet(xmlDOM, paste0("//Article[@PMID=", PMIDs[i], "]"))
  articleTitle <- xmlValue(curNode[[1]][["PubDetails"]][["ArticleTitle"]])
  issn <- xmlValue(curNode[[1]][["PubDetails"]][["Journal"]][["ISSN"]])
  volume <- xmlValue(curNode[[1]][["PubDetails"]][["Journal"]][["JournalIssue"]][["Volume"]])
  issue <- xmlValue(curNode[[1]][["PubDetails"]][["Journal"]][["JournalIssue"]][["Issue"]])
  journalID <- Journal.df[Journal.df$ISSN == issn & Journal.df$Volume == volume 
                          & Journal.df$Issue == issue, 1]
  Article.df[i, 1:3] <- c(PMIDs[i], journalID, articleTitle)

}
#Article.df


# Match pmid and author id
for (i in 1:length(PMIDs)) {
  curNode <- getNodeSet(xmlDOM, paste0("//Article[@PMID=", PMIDs[i], "]"))
  idx = 1
  while (!is.null(curNode[[1]][["PubDetails"]][["AuthorList"]][[idx]])) {
    lastName = xmlValue(curNode[[1]][["PubDetails"]][["AuthorList"]][[idx]][["LastName"]])
    foreName = xmlValue(curNode[[1]][["PubDetails"]][["AuthorList"]][[idx]][["ForeName"]])
    initials = xmlValue(curNode[[1]][["PubDetails"]][["AuthorList"]][[idx]][["Initials"]])
    authorID = Author.df$AuthorID[Author.df$LastName == lastName & Author.df$ForeName == foreName & Author.df$Initials == initials]
    if (length(authorID) == 0) {
      authorID = NA
    }
    Authorship.df[nrow(Authorship.df) + 1, ] <- c(PMIDs[i], authorID)
    idx = idx + 1
  }
}
# remove rows with missing authorID
Authorship.df <- na.omit(Authorship.df)
#Authorship.df



Journal.df$PubDate <- format(Journal.df$PubDate, "%Y-%m-%d")
dbWriteTable(dbcon, "Authors", Author.df, row.names = FALSE, overwrite = TRUE)
dbWriteTable(dbcon, "Journals", Journal.df, row.names = FALSE, overwrite = TRUE)
dbWriteTable(dbcon, "Articles", Article.df, row.names = FALSE, overwrite = TRUE)
dbWriteTable(dbcon, "Authorships", Authorship.df, row.names = FALSE, overwrite = TRUE)


Authors <- dbGetQuery(dbcon, "SELECT * FROM Authors")
Authors
result <- dbGetQuery(dbcon, "SELECT * FROM Journals WHERE ISSN='0006-2944'")
result
Journals <- dbGetQuery(dbcon, "SELECT * FROM Journals")
Journals
Articles <- dbGetQuery(dbcon, "SELECT * FROM Articles")
Articles
Authorships <- dbGetQuery(dbcon, "SELECT * FROM Authorships")
Authorships


#Disconnect
dbDisconnect(dbcon)
