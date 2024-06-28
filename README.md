code to test java bulk insertion and bulk transaction 

use it like this 

java -jar JavaTest.jar localhost Administrator password 10000 500 12 1000 0

localhost is the couchbase host 
Administrator is the couchbase username
password is the couchbase password
10000 is the number of documents
500 is the approximate size in bytes of the document
12 is a "version" key to prepend to the document key (which is a counter) in the insertion runs 
1000 is the buffer size used when there is a buffered operation
0 is the code of the operation we want to perform:

               case 0:
                    bulkInsert
                case 1:
                    bulkInsertWithBuffer
                case 2:
                    bulkTransaction
                case 3:
                    bulkTransactionReactive
                case 4:
                    bulkTransactionWithBuffer
                case 5:
                    bulkTransactionReactiveWithBuffer
                case 6:
                    bulkTransactionWithMonoReactive
