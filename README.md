code to test java bulk reactive transaction 

use it like this 

java -jar JavaTest.jar localhost Administrator password 10000 500 


localhost is the couchbase host 

Administrator is the couchbase username

password is the couchbase password

10000 is the number of documents

500 is the approximate size in bytes of the document

have a bucket called test, with a scope called test, and two collections, one called warmup and one called test. Flush after every run

