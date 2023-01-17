import apache_beam as beam
import time

# we create a pipeline with a PCollection of produce with a timestamp for their harvest date.

# We use Latest to get the element with the latest timestamp from the PCollection.

# https://beam.apache.org/documentation/transforms/python/elementwise/withtimestamps/

# We use Latest.Globally() to get the element with the latest timestamp in the entire PCollection


def to_unix_time(time_str, format='%Y-%m-%d %H:%M:%S'):
    struct_time = time.strptime(time_str, format)
    return time.mktime(struct_time) 

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

    timestamped = (
        products
        | 'beam.window.TimestampedValue takes a value and a Unix timestamp in the form of seconds.'
        >> beam.Map(lambda crop: beam.window.TimestampedValue(crop['item'], to_unix_time(crop['harvest'])))
    )

    # 'Get latest element'

    latest = (
        timestamped
        | "Get latest element"
        >> beam.combiners.Latest.Globally()
    )

    # print the latest 
    latest | 'print the latest element'>>beam.Map(print)

    # uncomment to print the timestamped collection
    # timestamped | 'print the timestamped collection'>>beam.Map(print)