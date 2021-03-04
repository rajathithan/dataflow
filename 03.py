import apache_beam as beam
from apache_beam import DoFn

class check(DoFn):
    def process(self, element, *args, **kwargs):
        for _ in range(element[1]):
            yield dict(zip(['name','value'],element))


with beam.Pipeline() as pipe:
    row = (
        pipe
        | beam.Create([
            ('apple', 1),
            ('banana', 2),
            ('orange', 3),

        ])
        | beam.ParDo(check())
        | beam.Map(print)
    )

'''
Output:

{'name': 'apple', 'value': 1}
{'name': 'banana', 'value': 2}
{'name': 'banana', 'value': 2}
{'name': 'orange', 'value': 3}
{'name': 'orange', 'value': 3}
{'name': 'orange', 'value': 3}

'''
