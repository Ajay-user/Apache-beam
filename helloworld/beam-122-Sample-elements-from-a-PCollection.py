import apache_beam as beam


# Sample elements from a PCollection

# Sample.FixedSizeGlobally() to get a fixed-size random sample of elements from the entire PCollection.
# We use Sample.FixedSizePerKey() to get fixed-size random samples for each unique key in a PCollection of key-values.


with beam.Pipeline('DirectRunner') as pipe:

    vegetables = (
        pipe
        | 'create vegetables'
        >> beam.Create([ 
          '🍓 Strawberry',
          '🍆 Eggplant',
          '🥕 Carrot',
          '🍆 Eggplant',
          '🍓 Strawberry',
          '🍅 Tomato',
          '🍅 Tomato',
          '🍓 Strawberry',
          '🥔 Potato',
          '🍅 Tomato',
          '🍓 Strawberry',
          '🍆 Eggplant',
          '🍓 Strawberry',
          '🍆 Eggplant',
          '🍆 Eggplant',
          '🥔 Potato',
          '🍆 Eggplant',
          '🍓 Strawberry',
          '🥔 Potato',
          '🥔 Potato',
          '🥕 Carrot',
          '🍓 Strawberry',
          '🍆 Eggplant',
          '🥕 Carrot',
          '🍆 Eggplant',
        ])
    )

    sampleGlobally = (
        vegetables
        | 'Sample.FixedSizeGlobally() to get a fixed-size random sample of elements from the entire PCollection'
        >> beam.combiners.Sample.FixedSizeGlobally(n=3)
    )

    # Print the sample
    # sampleGlobally | 'print the random sample '>> beam.Map(print)


    keyedCollection = (
        vegetables
        | 'split the text into (icon - text) pair'
        >> beam.Map(lambda txt: txt.split(' '))
        | 'swap key and value'
        >> beam.KvSwap()
    )

    sampleFixedSizePerKey = (
        keyedCollection
        | 'get fixed-size random samples for each unique key in a PCollection of key-values.'
        >> beam.combiners.Sample.FixedSizePerKey(n=3)
    )

    # print the sample
    sampleFixedSizePerKey|'random samples for each unique key'>>beam.Map(print)

