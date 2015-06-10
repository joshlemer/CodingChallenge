##Installation and usage

    You will need to have scala 2.11.4 installed

    To compile the project, from the project directory:

        scalac src/CodingChallenge.scala

    To run, you then go:

        scala CodingChallenge

    The output of the program will be in the results.txt file (the results
    from the sample csv is included in this project), and to change the
    source file name (the csv file), simply edit the 6th line in
    src/CodingChallenge.scala to reflect the new filename.


#The challenge text:

Data Analysis Coding Challenge

For this coding challenge we are tasking you with parsing and analyzing a data file full of energy utility meter readings. We receive a lot of data from utilities and it's important for us to be able to quickly analyze it and load it into our systems. Your script will be used to quickly summarize a large data file, giving context to its contents and quality.
Goal

    Parse a data file we might receive from a utility and calculate some simple summary statistics

Data File

    gzipped pipe delimited text file
    Each row represents a meter reading that a utility would use to generate a bill
    ElecOrGas column represents the resource measured: 1=Electricity, 2=Gas
    You may run into some bad lines. Handle them gracefully.
    Not all of the columns are important to analyze
    A small sample file (about 5000 lines) is available on Dropbox, but your solution should work for larger files (of at least 100K lines)

Date File Example:

CustID|ElecOrGas|Disconnect Doc|Move In Date|Move Out Date|Bill Year|Bill Month|Span Days|Meter Read Date|Meter Read Type|Consumption|Exception Code
108000601|1|N|20080529|99991231|2011|6|33|20110606|A|28320|
108000601|1|N|20080529|99991231|2011|7|29|20110705|A|13760|
108000601|1|N|20080529|99991231|2011|8|30|20110804|A|16240|
108000601|1|N|20080529|99991231|2011|9|28|20110901|A|12560|
108000601|1|N|20080529|99991231|2011|10|33|20111004|A|12400|
108000601|1|N|20080529|99991231|2011|11|28|20111101|A|9440|
108000601|1|N|20080529|99991231|2011|12|34|20111205|A|12160|
108000601|1|N|20080529|99991231|2012|1|32|20120106|A|11360|
108000601|1|N|20080529|99991231|2012|2|31|20120206|A|10480|

Statistics to Calculate:

1: Number of unique customers

2: Breakdown of the number of customers that have electricity only, gas only, and both electricity and gas

3: Breakdown of the number of meter readings per customer (split by electricity and gas) that can be used to generate a histogram

E.g.:
 Electricity
  Number of meter readings: Number of customers
    1: 2
    2: 12
    3: 19
    4: 12
    5: 24
    6: 100
    7: 88
    8: 1200
    9: 2200
    10: 3000
    11: 2999
    12: 4550
    13:100
    ...

4: Average consumption per Bill Month per resource across all customers

E.g.:
 Electricity
    January (all years): 10033.2
    February (all years): 8022.3
    March (all years): 12022.7
    ...
