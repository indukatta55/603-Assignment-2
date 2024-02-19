from mrjob.job import MRJob
import csv

class CountPerMonth(MRJob):

    def mapper(self, _, line):
        #parse the input line as a CSV row
        row = next(csv.reader([line]))
        #extract the date field
        date = row[2]  
        #extract the year and month
        year_month = date[:7]  
        #emit key-value pair with year-month as key and count 1 as value
        yield year_month, 1

    def reducer(self, year_month, counts):
        total_count = sum(counts)
        yield year_month, total_count

if __name__ == '__main__':
    CountPerMonth.run()
