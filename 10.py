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
        | beam.Map(print)
    )

'''
Output:

(('apple', 'today'), (5, 18, 3.6))
(('cherry', 'yesterday'), (10, 24, 2.4))
(('apple', 'yesterday'), (4, 8, 2.0))
(('cherry', 'today'), (4, 14, 3.5))
(('banana', 'yesterday'), (1, 2, 2.0))
(('banana', 'today'), (1, 2, 2.0))

'''