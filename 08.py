import apache_beam as beam


with beam.Pipeline() as pipe:
    row = (
        pipe
        | beam.Create([
            ('apple', (1,2)),
            ('banana', (3,2)),
            ('orange', (4,3)),
            ('apple', (6,5)),
            ('banana', (7,3)),
            ('orange', (8,4)),

        ])
        | beam.CombinePerKey(beam.combiners.TupleCombineFn(sum, max))
        | beam.Map(print)
    )

'''
Output:

('apple', (7, 5))
('banana', (10, 3))
('orange', (12, 4))

'''