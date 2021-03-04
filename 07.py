import apache_beam as beam


with beam.Pipeline() as pipe:
    row = (
        pipe
        | beam.Create([
            ('apple', 1),
            ('banana', 2),
            ('orange', 3),
            ('apple', 1),
            ('banana', 2),
            ('orange', 3),

        ])
        | beam.CombinePerKey(sum)
        | beam.Map(print)
    )

'''
Answers

('apple', 2)
('banana', 4)
('orange', 6)

'''