from mrjob.job import MRJob
import csv
from statistics import mean
import re

class cool(MRJob):

    def mapper(self, _, line):
        row = next(csv.reader([line]))
        id, business_id, date, review_id, stars, text, type, user_id, cool, useful, funny = row
        if re.match(r'^[0-9]+$', stars) and int(cool) != 0:
            yield None, int(stars)

    def reducer(self, _, star_ratings):
        # Computing the average star rating of cool reviews
        ratings = list(star_ratings)
        if ratings:
            yield "average", mean(ratings)

if __name__ == '__main__':
    cool.run()
