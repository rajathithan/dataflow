import apache_beam as beam
import os


with beam.Pipeline() as pipe:
    fruits = (
        pipe
        | beam.io.ReadFromText('fruits.csv')
        | beam.Map(lambda line: str(line).strip().split(','))
        | beam.Map(lambda row: row[:2] + [int(row[2])])
        | beam.Map(lambda row: (tuple(row[:2]), tuple(row[2:] * 3)))
        | beam.Map(print)
        )


'''

output:

(('apple', 'today'), (4, 4, 4))   
(('cherry', 'yesterday'), (1, 1, 1))
(('apple', 'yesterday'), (1, 1, 1))
(('apple', 'yesterday'), (1, 1, 1))
(('cherry', 'today'), (4, 4, 4))
(('apple', 'yesterday'), (1, 1, 1))
(('cherry', 'today'), (3, 3, 3))
(('apple', 'today'), (4, 4, 4))
(('cherry', 'today'), (4, 4, 4))
(('cherry', 'yesterday'), (2, 2, 2))
(('apple', 'today'), (5, 5, 5))
(('apple', 'yesterday'), (5, 5, 5))
(('cherry', 'yesterday'), (4, 4, 4))
(('banana', 'yesterday'), (2, 2, 2))
(('apple', 'today'), (1, 1, 1))
(('cherry', 'today'), (3, 3, 3))
(('cherry', 'yesterday'), (5, 5, 5))
(('banana', 'today'), (2, 2, 2))
(('cherry', 'yesterday'), (1, 1, 1))
(('cherry', 'yesterday'), (2, 2, 2))
(('apple', 'today'), (4, 4, 4))
(('cherry', 'yesterday'), (2, 2, 2))
(('cherry', 'yesterday'), (2, 2, 2))
(('cherry', 'yesterday'), (1, 1, 1))
(('cherry', 'yesterday'), (4, 4, 4))

'''