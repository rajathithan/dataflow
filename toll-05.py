
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


def key_by_tollbooth_month(element):
    # create a multiple key (tollbooth, month) key with single value (total) tuple
    return (element['tollbooth'], element['month']), element['total']


class ParseRecordsAndAddTotals(beam.DoFn):
    
    def process(self, element, nutprices, *args, **kwargs):
        # convert values into correct data types
        record_date = datetime.strptime(element['date'], '%Y.%m.%d')  # parse date
        element['tollbooth'] = int(element['tollbooth'])
        element['cornsilk'] = int(element['cornsilk'])
        element['slate_gray'] = int(element['slate_gray'])
        element['navajo_white'] = int(element['navajo_white'])

        # add calculated columns: total toll, week of year, and month
        element['total'] = (
                (nutprices['cornsilk'] * element['cornsilk']) +
                (nutprices['slate_gray'] * element['slate_gray']) +
                (nutprices['navajo_white'] * element['navajo_white'])
        )
        element['week'] = record_date.isocalendar()[1]      # week number in year
        element['month'] = record_date.strftime("%Y.%m")

        yield element



def run():
    print("Town of Squirreliwink Bureau Of Tolls and Nuts Affair\n\n[PART-1]")
    # parse command line args:
    #   - parse both beam args and known script args
    parser = argparse.ArgumentParser(description="Town of Squirreliwink Bureau Of Tolls and Nuts Affair")

    parser.add_argument('-i', '--input', type=str,
                        default='./',
                        help='Input Path')

    parser.add_argument('-o', '--output', type=str,
                        default='./',
                        help='Output Path')

    known_args, beam_args = parser.parse_known_args(sys.argv)

    # construct pipeline and run
    options = PipelineOptions(beam_args)
    with beam.Pipeline(options=options) as pipeline:


        # Create a Pcollection of nut prices
        logger.info("creating the inflated toll fees")
        nut_prices = (pipeline
                        | beam.Create([
                            ('cornsilk',2.0),
                            ('slate_gray',3.5),
                            ('navajo_white',7.0),
                        ])
        )

        

        logger.info("reading csv file and parsing records")
        # read input file, separate csv columns, parse and add totals
        records = (pipeline
                   | beam.io.ReadFromText(os.path.join(known_args.input, 'tollbooth_logs.csv'),
                                          skip_header_lines=1)
                   | beam.Map(parse_csv)
                   | beam.ParDo(ParseRecordsAndAddTotals(),nutprices=beam.pvalue.AsDict(nut_prices))
                   )


        logger.info("summing by tollbooth and month...")
        records = (records
                   | beam.Map(key_by_tollbooth_month)       # multi-key by (tollbooth, month)
                   | beam.CombinePerKey(sum)                # sum by key
                   | beam.io.WriteToText(os.path.join(known_args.output, "output-1"),
                               file_name_suffix='.txt',
                               header='tollbooth,month,total')
                   )





if __name__ == '__main__':
    run()