
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

CHECK_HEADERS = 'tollbooth,month,total'


# Create a dictonary of values
def parse_csv(line, headers):
    values = [v.strip(' "\n') for v in str(line).split(',')]
    keys = headers.split(',')
    return dict(zip(keys, values))

# Create tuple key Value pairs
def tuple_csv(line, headers):
    values = [v.strip(' "\n') for v in str(line).split(',')]
    return (int(values[0]), values[1]), float(values[2])

# Create key Value pairs
def key_by_tollbooth_month(element):
    return (int(element['tollbooth']), element['month']), float(element['total'])

# Identify the difference amount
def finddifference(element):
    return element[0] , (element[1][0][0]-element[1][1][0])

# Filter the nonzero values
def filternonzero(element):
    if element[1] != 0:
        element =  (element[0][0] , element[1])
        element = ','.join([str(k) for k in element])
        return element

# Identify the total amount swindled
def identifyculprit(element):
    t = 0
    toll = 0 
    for x in element:
        if x is not None:
            l = x.split(',')
            toll = l[0]
            t = t + float(l[1])
    return f"Total amount swindled by tollbooth- {toll} for the entire year is ${t} "
        

# Append the dictionary with total price and month
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
    print("Identify the toll-culprit from the Town of Squirreliwink Bureau Of Tolls and Nuts Affair")

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

        # Create a Pcollection of the actual records
        def records_pipeline(csvfile):
            return (
                pipeline
                   | "Reading tollbooth records" >> beam.io.ReadFromText(os.path.join(known_args.input, csvfile), skip_header_lines=1)
                   | "Create the dictionary" >> beam.Map(parse_csv, headers=TOLLBOOTH_HEADERS)
                   | "Calculate the prices" >> beam.ParDo(ParseRecordsAndAddTotals(),nutprices=beam.pvalue.AsDict(nut_prices))
                   | "Pair month & tollbooth as a key" >> beam.Map(key_by_tollbooth_month)      
                   | "Do summation by Key" >> beam.CombinePerKey(sum)
            )

        # Create a Pcollection of the expected records
        def checkrecords_pipeline(csvfile):
            return (
                pipeline
                    | "Reading tollbooth check records" >> beam.io.ReadFromText(os.path.join(known_args.input, csvfile),skip_header_lines=1)
                    | "Convert them to tuple kv pairs" >> beam.Map(tuple_csv, headers=CHECK_HEADERS)

            )


        records = records_pipeline('tollbooth_logs.csv')

        checkrecords =  checkrecords_pipeline('tollbooth_check_file.csv')

        # Combine both the pcollections and CoGroupByKey 
        results = (
                    (records, checkrecords)  
                    | 'group_by_name' >> beam.CoGroupByKey() 
                    | "find the total difference" >> beam.Map(finddifference)
                    | "filter only non-zero values" >> beam.Map(filternonzero)
                    | "Convert the values to a list" >> beam.combiners.ToList() 
                    | "Identify the total" >> beam.Map(identifyculprit)
                    | "Write to file" >> beam.io.WriteToText(os.path.join(known_args.output, "swindled-amount"),file_name_suffix='.txt')
        )
    
        
  

if __name__ == '__main__':
    run()