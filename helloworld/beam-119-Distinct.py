import apache_beam as beam


# Produces a collection containing distinct elements of the input collection.


with beam.Pipeline('DirectRunner') as pipe:

    vegetables = (
        pipe
        |"create vegetables"
        >>beam.Create([ 
          '🥕',
          '🥕',
          '🍆',
          '🍅',
          '🍅',
          '🍅',
        ])
    )

    distinct_elem = (
        vegetables
        | "Produces a collection containing distinct elements of the input collection"
        >> beam.Distinct()
        | "print distinct"
        >> beam.Map(print)
    )