import apache_beam as beam

with beam.Pipeline() as pipe:
    row = (
        pipe
        | beam.Create([
            ('apple', 1),
            ('banana', 2),
            ('orange', 3),

        ])
        | beam.Map(lambda x: (x[0], x[1] ** 2))
        | beam.Map(print)
    )

'''
Output:

('apple', 1)
('banana', 4)
('orange', 9)

'''