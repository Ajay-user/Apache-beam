import apache_beam as beam

with beam.Pipeline() as pipeline:

    plants = (
        pipeline
        | 'Create plants'
        >> beam.Create(['🍓', '🥕', '🥕', '🥕', '🍆', '🍆', '🍅', '🍅', '🍅', '🌽'])
    )

    total = (
        plants
        | ' Count all elements in a PCollection, even if there are duplicate elements.'
        >> beam.combiners.Count.Globally()
        | ' We use Count.Globally() to count all elements in a PCollection '
        >> beam.Map(print)
    )



