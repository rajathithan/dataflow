import apache_beam as beam


with beam.Pipeline() as pipe:
    row = (
        pipe
        | beam.Create([
            ('apple', 1),
            ('banana', 2),
            ('orange', 3),

        ])
        | beam.Map(lambda element, power: (element[0],element[1] ** power), power=3)
        | beam.Map(print)
    )

'''
Output:

('apple', 1)
('banana', 8)
('orange', 27)
'''