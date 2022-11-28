import apache_beam as beam

with beam.Pipeline() as pipeline:


    seasonal_plants = (
        pipeline
        | 'Create some seasonal plants'
        >> beam.Create([ 
        ('spring', '🍓'),
        ('spring', '🥕'),
        ('summer', '🥕'),
        ('fall', '🥕'),
        ('spring', '🍆'),
        ('winter', '🍆'),
        ('spring', '🍅'),
        ('summer', '🍅'),
        ('fall', '🍅'),
        ('summer', '🌽'),
    ])
    )


    countPerSeason = (
        seasonal_plants
        | "Count the elements for each unique key in a PCollection of key-values"
        >> beam.combiners.Count.PerKey()
    )

    countPerSeason|beam.Map(print)

    