Word counter without parallelization

WordGenerator -> Splitter -> Counter -> Printer
grouping:	  S           F          G

1. WordGenerator generates short sentences and sends end of stream sign
- shuffle grouping guarantees that each bolt replica gets equal load share
2. Splitter does splitting sentences into words
- field grouping does grouping so that all words that are the same go to the same designated replica 
3. Counter does counting of distinct words
- all bolts replicas go to the input of the common bolt
4. Printer does counter output once end of stream marker is received