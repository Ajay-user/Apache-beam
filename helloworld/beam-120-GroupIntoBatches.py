import apache_beam as beam

# We use GroupIntoBatches to get fixed-sized batches for every key,
# outputs a list of elements for every key.

# For unkeyed data and dynamic batch sizes, one may want to use BatchElements.



with beam.Pipeline() as pipe:

    products = (
        pipe
        |'create products'
        >>beam.Create([ 
          ('spring', '🍓'),
          ('spring', '🥕'),
          ('spring', '🍆'),
          ('spring', '🍅'),
          ('summer', '🥕'),
          ('summer', '🍅'),
          ('summer', '🌽'),
          ('fall', '🥕'),
          ('fall', '🍅'),
          ('winter', '🍆'),
        ])
    )

    batches = (
        products
        |'get fixed-sized batches for every key'
        >>beam.GroupIntoBatches(batch_size=3)
        |'GroupIntoBatches outputs a list of elements for every key.'
        >>beam.Map(print)
    )


    vegetables = (
        pipe
        | ' create vegetables'
        >> beam.Create([ 
            '🥕', '🍆', '🥕', '🍅', '🌽', '🥕', '🍆', '🥕', '🍅', '🌽', '🍅', '🌽', '🥕', '🍆'
        ])
    )

    batchElems = (
        vegetables
        |'For unkeyed data and dynamic batch sizes, one may want to use BatchElements.'
        >>beam.BatchElements(max_batch_size=3)
    )

    # un-comment to print output
    # batchElems|'batch of veg'>>beam.Map(print)


