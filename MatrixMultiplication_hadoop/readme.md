Matrix Multiplication

Logic:  The output from map phase is set as <key,value> pair, where the key represents the output cell location (0,0) , (0,1) etc.. and value will be the list of all values required for reducer to do computation. 

e.g. 

In matrix multiplication the first cell of output i.e. (0,0) has multiplication and summation of elements from row 0 of the matrix A and elements from col 0 of matrix B.  
The key of the mapper is set as (0,0) and the value is set as the array of values from row 0 of A and column 0 of B. This is passed to the reducer and then the computation is done for the value in the output cell (0,0).

The input and output is set for a 5x5 matrix. But the logic can be scaled to whatever dimensions of the matrix is desired.

Mapper name  MMapper.java

Reducer name  MReducer.java

Runner name  MRunner.java
