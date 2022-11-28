import apache_beam as beam

# CombinePerKey

# Combines all elements for each key in a collection.


with beam.Pipeline('DirectRunner') as pipe:

    products=(
        pipe
        |"create some products"
        >>beam.Create([
          ('🥕', 3),
          ('🥕', 2),
          ('🍆', 1),
          ('🍅', 4),
          ('🍅', 5),
          ('🍅', 3),

        ])
    )

    #  sum which takes an iterable of numbers and adds them together.
    productCount = (
        products
        |"Combines all elements for each key in a collection."
        >>beam.CombinePerKey(sum)
        |"print the result"
        >>beam.Map(print)

    )

    #  Combining with a lambda function
    productCount = (
        products
        |'combine per key - Combining with a lambda function'
        >>beam.CombinePerKey(lambda val: sum(val))
    )

    # un-comment to see the results
    # productCount| "count" >> beam.Map(print)


    # Combining with multiple arguments

    productThreshold = (
    products
    |'combine per key - Combining with multiple arguments'
    >>beam.CombinePerKey(lambda val, threshold:min(sum(val), threshold), threshold=5)
    )

    # un-comment to see the results
    # productThreshold|"multiple arguments" >> beam.Map(print)
