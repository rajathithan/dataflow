
import sys
import os
import logging
import argparse
from datetime import datetime

import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions

# setup python logging
logging.basicConfig(format='[%(levelname)-8s] [%(asctime)s] [%(module)-35s][%(lineno)04d] : %(message)s', level=logging.INFO)
logger = logging

TOLLBOOTH_HEADERS = 'date,tollbooth,license_plate,cornsilk,slate_gray,navajo_white'

def parse_csv(line):
    # breakout csv values into a list and strip out space, ", and carriage return
    values = [v.strip(' "\n') for v in str(line).split(',')]
    keys = TOLLBOOTH_HEADERS.split(',')
    # pack row in {key: value} dict with column values
    return dict(zip(keys, values))



def run():
    print("Town of Squirreliwink Bureau Of Tolls and Nuts Affair\n\n[PART-1]")
    # parse command line args:
    #   - parse both beam args and known script args
    parser = argparse.ArgumentParser(description="Town of Squirreliwink Bureau Of Tolls and Nuts Affair")
    parser.add_argument('-i', '--input', type=str,
                        default='./',
                        help='Input Path')
    known_args, beam_args = parser.parse_known_args(sys.argv)

    # construct pipeline and run
    options = PipelineOptions(beam_args)
    with beam.Pipeline(options=options) as pipeline:
        logger.info("reading csv file and parsing records")
        # read input file, separate csv columns, parse and add totals
        records = (pipeline
                   | beam.io.ReadFromText(os.path.join(known_args.input, 'tollbooth_logs.csv'),
                                          skip_header_lines=1)
                   | beam.Map(parse_csv)
                   )

        # print out
        logger.info("dumping pcollection to print")
        printout = (records | beam.Map(print))


if __name__ == '__main__':
    run()