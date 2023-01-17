import apache_beam as beam
import time

# We use Latest.Globally() to get the element with the latest timestamp in the entire PCollection
# We use Latest.PerKey() to get the elements with the latest timestamp for each key in a PCollection of key-values.

def to_unix_time(time_str, format='%Y-%m-%d %H:%M:%S'):
    struct_time = time.strptime(time_str, format)
    return time.mktime(struct_time) 

with beam.Pipeline() as pipe:

    products = (
        pipe
        | 'create produce'
        >> beam.Create( 
        [ 
          ('spring', {
              'item': '🥕', 'harvest': '2020-06-28 00:00:00'
          }),
          ('spring', {
              'item': '🍓', 'harvest': '2020-06-16 00:00:00'
          }),
          ('summer', {
              'item': '🥕', 'harvest': '2020-07-17 00:00:00'
          }),
          ('summer', {
              'item': '🍓', 'harvest': '2020-08-26 00:00:00'
          }),
          ('summer', {
              'item': '🍆', 'harvest': '2020-09-04 00:00:00'
          }),
          ('summer', {
              'item': '🥬', 'harvest': '2020-09-18 00:00:00'
          }),
          ('summer', {
              'item': '🍅', 'harvest': '2020-09-22 00:00:00'
          }),
          ('autumn', {
              'item': '🍅', 'harvest': '2020-10-01 00:00:00'
          }),
          ('autumn', {
              'item': '🥬', 'harvest': '2020-10-20 00:00:00'
          }),
          ('autumn', {
              'item': '🍆', 'harvest': '2020-10-26 00:00:00'
          }),
          ('winter', {
              'item': '🥬', 'harvest': '2020-02-24 00:00:00'
          }),
        ]
        )
    )

    timestamped = (
        products
        | 'beam.window.TimestampedValue takes a value and a Unix timestamp in the form of seconds.'
        >> beam.Map(lambda pair: beam.window.TimestampedValue(
           #( (# key , #value)                 # Unix timestamp)
            (pair[0], pair[1]['item']), to_unix_time(pair[1]['harvest'])
            )
        )
    )

    # 'Get latest element'

    latest = (
        timestamped
        | "Get latest element"
        >> beam.combiners.Latest.Globally()
    )

    # uncomment to print the latest  output : ('autumn', '🍆')
    # latest | 'print the latest element'>>beam.Map(print)

    # to print the latest per key
    latestPerKey=(
        timestamped
        | "Get the elements with the latest timestamp for each key in a PCollection of key-values."
        >>beam.combiners.Latest.PerKey()
    )

    # uncomment to print the latest per key 
    # output : 
    # ('spring', '🥕')
    # ('summer', '🍅')
    # ('autumn', '🍆')
    # ('winter', '🥬') 
    latestPerKey | 'print the latest element'>>beam.Map(print)   
