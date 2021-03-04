import apache_beam as beam
import os


with beam.Pipeline() as pipe:
    fruits = (
        pipe
        | beam.io.ReadFromText('fruits.csv')
        | beam.Map(lambda line: str(line).strip().split(','))
        | beam.Map(lambda row: row[:2] + [int(row[2])])
        | beam.Map(lambda row: (tuple(row[:2]), tuple(row[2:] * 3)))
        )

    total = (
        fruits
        | beam.CombinePerKey(
            beam.combiners.TupleCombineFn(beam.combiners.CountCombineFn(), sum, beam.combiners.MeanCombineFn())
        )
    )

    
    pipe = ( total
    # Combine the two tuples
    | beam.Map(lambda jointuple: jointuple[0] + jointuple[1])
    # Join all the tuple values and convert them to string
    | beam.Map(lambda joinall: ','.join([str(_) for _ in joinall]))
    | beam.io.WriteToText('fruit_total', file_name_suffix='.txt')
    )