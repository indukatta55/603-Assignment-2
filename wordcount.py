from mrjob.job import MRJob
import csv

class AvgWordCount(MRJob):

    def mapper(self, _, line):
        #parse the input line as a CSV row
        row = next(csv.reader([line]))
        text = row[5]
        yield "word_count", len(text.split())

    def reducer(self, key, values):
        total_words = 0
        total_reviews = 0
        #iterating through the word counts and counting the total number of words
        #also count the total number of reviews
        for word_count in values:
            total_words += word_count
            total_reviews += 1
        #average number of words per review
        average_words_per_review = total_words / total_reviews
        yield "average", average_words_per_review

if __name__ == '__main__':
    AvgWordCount.run()
