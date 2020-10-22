CS441 - Homework 2
---
Homework 2: Gain experience with a distributed computational problem. Designed and implemented an instance of the map/reduce computational model and deployed the custom jar at AWS EMR
---
Name : Karan Venkatesh Davanam (kdavan2)
---

### Overview

* As part this homework, the objective was to process the dblp.xml dataset using Hadoop map-reduce framework. The data set is present in the URL: https://dblp.uni-trier.de/xml/

* The second part of the homework was to deploy the map-reduce program at AWS EMR. The youtube link will explain the step-by-step process https://www.youtube.com/watch?v=WNTl-23QOhc

* There were five tasks to complete as part of this homework and the respective class names are given below:

       1) top ten published authors at each venue - AuthorVenueCount
       
       2) list of authors who published without interruption for N years where 10 <= N - AuthorConsecutiveYears
       
       3) list of publications that contains only one author - PublicationSingleAuthor
       
       4) list of publications for each venue that contain the highest number of authors for each of these venues - PublicationWithHighestAuthors
       
       5) list of top 100 authors in the descending order who publish with most co-authors and the list of 100 authors who publish without any co-authors - CoAuthorCount
       
* In my application.conf I have set both the input and output path. After each run delete the output folder to avoid error during runtime. The path represents path in HDFS file-system and it holds good for both AWS EMR and HDP sandbox.

       1) jobInputPath=/home/hadoop/input (should be created by user in HDFS) 
       
       2) jobOutputPath=/home/hadoop/output (code creates the output folder , but user should delete after collecting the results{csv files} after each run)
       
* While running  jar file please note that it does not take any input arguements. 

* In setting the project to run the jar I have given the steps for both AWS and HDP VM you can choose either one of them.
       
### Environment and Prerequisites

* Development environment : Windows 10, Intellij, SBT, Scala - 2.13.3.

* Running Jar environment : AWS EMR instance, Horton's VM

* Hypervisor : VMware-workstation-full-16.0.0-16894299

### Creating fat jar file

* Command: sbt clean compile assembly (after cloning the project from github)

### Steps to run the executable on HDP sandbox VMware (for AWS EMR the step-by-step approach is in the youtube video)

* Use SecureCRT or directly use the scp and ssh command on port -2222 to transfer files and to login to the VM.

        1) Start the Horton's box on the VM or check if your cluster in EMR is running
        
        2) If using SecureCRT login into the VM using root user and port -2222 or if logging into AWS EMR use Putty and login using ppk file as hadoop user.
        

* Steps for HDP Sandbox

        1) Get the dblp.xml and the fat-jar(karanvenkatesh_davanam_cs441_hw2/target/scala-2.13/hadoop-map-reduce-scala-sbt-assembly-fatjar-1.0.jar) file from local to the VM box using previous step.
        
        2) Now both the files will be under /root
        
        3) Do a chmod 755 on both the files  and copy the files to /home/hdfs
        
        4) su hdfs
        
        5) cd /home/hdfs
        
        5) hdfs dfs -mkdir /home
        
        6) hdfs dfs -mkdir /home/hadoop
        
        7) hdfs dfs -mkdir /home/hadoop/input
        
        8) hdfs dfs -put (path where dblp.xml exists)  /home/hadoop/input/
        
        9) hadoop jar hadoop-map-reduce-scala-sbt-assembly-fatjar-1.0.jar
        
        10) hdfs dfs -ls /home/hadoop/output/*.csv (you will see 5 csv files)
        
* Steps for AWS EMR Master instance box:

        1) Using putty and .ppk file login to the master instance as hadoop user
        
        2) Using winscp get the jar and dblp.xml from your local to the master box.
        
        3) hdfs dfs -mkdir /home
        
        4) hdfs dfs -mkdir /home/hadoop
        
        5) hdfs dfs -mkdir /home/hadoop/input
        
        6) hdfs dfs -put (path where dblp.xml exists)  /home/hadoop/input/
        
        7) Run the jobs from steps tab in AWS EMR dashboard
        
        8) To get the files from hdfs file-system : hdfs dfs -get /home/hadoop/output/*.csv
        
      
### Description of the output path produced by the program

       1) The output path /home/hadoop/output will contain five csv files. (author_consecutive_years.csv, author_venue_count.csv, co_author_count_highest_lowest.csv, publication_single_author.csv, publications_with_highest_authors.csv) 
       
       2) These csv are getting created by the utility method writeCsv(), it is collecting the outputs of a particular job and writing it to a single csv file for each map-reduce job
       
### Description of mapper and reducer of each job          


1)  author_consecutive_years (AuthorConsecutiveYears)

     - Map : The function inside the mapper class emits (author,year) year is the publication year. The getAuthorList gets the author list for each element and getPublicationYear gets the publication year. Then by iterating through the author list each author is tagged to the year value.
     
     - Reduce: For each author key  a reducer will be created and the list of years that is tagged to each author is iterated using foreach to list of consecutive years. (>=10)

2)   author_venue_count (AuthorVenueCount)

     - Map : The map emits (venue,author)
     
     - Reduce: So foreach venue key a reducer is created. The reducer will receive a list of authors as value and venue as key. Using hashmap the author and the number of times he/she appear in the list is stored. The hashmap is sorted in descending order and top 10 authors from each venue is retrieved.
     
3)  publication_single_author (PublicationSingleAuthor)

     - Map : The map emits (venue,publicationtitle) , the publication title is value from the title for elements having only one author or editor tag.
     
     - Reduce : For each venue key a reducer is created and the list of publication list is iterated over to emit (venue,title)
     
4)  publications_with_highest_authors (PublicationWithHighestAuthor)

     - Map : The map emits (venue,publicationtitle), the publicationtile List is returned from getPublicationList() and this list is created based on number of author tags in each element. So if there are three author tag the publicationList will have the same title 3 times. Then this list is iterated over to emit (venue,title)
     
     - Reduce : For each venue key a reducer is created and using hashmap the publication count is calculated based on  the number of times it appears in the reducers value list and sorted to find the highest number of authors for list of publication in each venue
     
5)  co_author_count_highest_lowest (CoAuthorCount)
     
     - For this job only one reducer is set and cleanup() function is used to sort the final list
     
     - The cleanup function is findind the top 100 and bottom 100 and writing to the same csv file
     
     - The map will emit (author,co-author) tuple : example author1 and author2 exits in the xml element then the map function will emit (auhtor1,author2) and vice-versa
     
     - To the reducer the input will be (author,list(co-author)) and using hashmap the author and it's co-author list length is stored and sorting happens in  the cleanup function.
     
        
### Note for Credits
    
    - The XMLPublicationInputFormat was written referring to [Pramodh Acharya Implementation](https://github.com/pramo31/MapReduce-DBLPDataSet/blob/master/src/main/scala/com/cloud/mapreduce/paser/XmlInputFormat.scala) and standard XMLinput from Mahout's. 
    
### Future Improvements

    - Should improve the code that writes the data into CSV file now I'm using input IOUtils
    
### Output CSV for each job for dblp.xml dataset is present in result.zip     
       
   

  
       
       


