# Counting all unique elements

import apache_beam as beam

with beam.Pipeline() as pipeline:

    products = (
        pipeline
        | 'create products'
        >> beam.Create(['🍓', '🥕', '🥕', '🥕', '🍆', '🍆', '🍅', '🍅', '🍅', '🌽'])
    )

    uniqueProducts = (
        products
        | ' We use Count.PerElement() to count the only the unique elements in a PCollection. '
        >> beam.combiners.Count.PerElement()
    )

    uniqueProducts|beam.Map(print)