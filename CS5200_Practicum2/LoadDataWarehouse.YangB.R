#Name: Bingqian Yang
#Course: CS5200 Spr 2023
#Date: 4/18/2023


#Part 2
library(RMySQL)
library(DBI)
library(RSQLite)
library(sqldf)


#Connect to MySQL database
db_user <- 'root'
db_password <- rstudioapi::askForPassword()
db_host <- 'localhost' 
db_port <- 3306
db_con <-  dbConnect(MySQL(), user = db_user, password = db_password, host = db_host, port = db_port)
# Dropping db if it exists
dbExecute(db_con,"DROP DATABASE IF EXISTS Practicum2")
# Creating db otherwise
dbExecute(db_con,"CREATE DATABASE Practicum2")
dbExecute(db_con,"USE Practicum2")



#Create a star schema for author facts
dbExecute(db_con,"DROP TABLE IF EXISTS AuthorFacts")
dbExecute(db_con,"CREATE TABLE AuthorFacts(
          AuthorID INTEGER PRIMARY KEY,
          LastName VARCHAR(255),
          ForeName VARCHAR(255),
          Initials VARCHAR(20),
          ArticleNum INTEGER,
          CoAuthorNum INTEGER
          )")

#part 1 connection
part1_con <- dbConnect(RSQLite::SQLite(), "part1.db")

#Populate author facts table
AuthorFacts.df <- dbGetQuery(part1_con, "
  SELECT a.AuthorID, a.LastName, a.ForeName, a.Initials,
  COUNT(DISTINCT af.PMID) as ArticleNum, 
  COALESCE((SELECT COUNT(DISTINCT af2.AuthorID) 
            FROM Authorships af2 
            WHERE af.PMID = af2.PMID AND af2.AuthorID <> a.AuthorID), 0) as CoAuthorNum
  FROM Authors a 
  JOIN Authorships af ON a.AuthorID = af.AuthorID 
  GROUP BY a.AuthorID")
#AuthorFacts.df

# Write to MySQL database
dbSendQuery(db_con, "SET GLOBAL local_infile = true;")
dbWriteTable(db_con,"AuthorFacts", AuthorFacts.df, append=TRUE, row.names=FALSE)
dbGetQuery(db_con,"SELECT * FROM AuthorFacts")



#Create and populate a star schema for journal facts
dbExecute(db_con,"DROP TABLE IF EXISTS JournalFacts")
dbExecute(db_con, "CREATE TABLE JournalFacts(
                    JournalID INTEGER PRIMARY KEY,
                    ISSN VARCHAR(50),
                    JournalName VARCHAR(255),
                    Volume INTEGER,
                    Issue INTEGER,
                    ArticleNum INTEGER,
                    Year INTEGER,
                    Quarter INTEGER,
                    Month INTEGER
                  )")

JournalFacts.df <- dbGetQuery(part1_con, "
 SELECT j.JournalID, j.ISSN, j.Title as JournalName, j.Volume, j.Issue,
       COUNT(DISTINCT a.PMID) as ArticleNum,
       strftime('%Y', j.PubDate) as Year,
       strftime('%m', j.PubDate) as Month,
       ((strftime('%m', j.PubDate) - 1) / 3) + 1 as Quarter
FROM Articles a
JOIN Journals j ON a.JournalID = j.JournalID
JOIN Authorships af ON a.PMID = af.PMID
GROUP BY j.JournalID, Year, Quarter, Month;")
#JournalFacts.df

# Write to MySQL database
dbWriteTable(db_con,"JournalFacts", JournalFacts.df, append=TRUE, row.names=FALSE)
dbGetQuery(db_con,"SELECT * FROM JournalFacts")



#prepare for part 3
dbExecute(db_con,"DROP TABLE IF EXISTS ArticleDim")
dbExecute(db_con, "CREATE TABLE ArticleDim (
          PMID INTEGER PRIMARY KEY,
          JournalID INTEGER
          )")
ArticleDim.df <- dbGetQuery(part1_con,"SELECT PMID, JournalID FROM Articles")
dbWriteTable(db_con,"ArticleDim", ArticleDim.df, append=TRUE, row.names=FALSE)
dbGetQuery(db_con,"SELECT * FROM ArticleDim")



dbDisconnect(part1_con)
dbDisconnect(db_con)
