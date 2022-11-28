import apache_beam as beam

# we create a pipeline with a PCollection of produce with a timestamp for their harvest date.

# We use Latest to get the element with the latest timestamp from the PCollection.

# https://beam.apache.org/documentation/transforms/python/elementwise/withtimestamps/

with beam.Pipeline() as pipe:

    products = (
        pipe
        | 'create produce'
        >> beam.Create( 
        [ 
          {
              'item': '🥬', 'harvest': '2020-02-24 00:00:00'
          },
          {
              'item': '🍓', 'harvest': '2020-06-16 00:00:00'
          },
          {
              'item': '🥕', 'harvest': '2020-07-17 00:00:00'
          },
          {
              'item': '🍆', 'harvest': '2020-10-26 00:00:00'
          },
          {
              'item': '🍅', 'harvest': '2020-10-01 00:00:00'
          },
        ]
        )
    )
