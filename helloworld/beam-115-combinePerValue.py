import apache_beam as beam

# beam.CombineValues
# Combines an iterable of values in a keyed collection of elements

with beam.Pipeline() as pipeline:
    products = (
      pipeline
      |'Create produce counts' 
      >> beam.Create([
          ('🥕', [3, 2]),
          ('🍆', [1]),
          ('🍅', [4, 5, 3]),
        ])
    )

    #   sum of values 

    total_products =(
        products
        |'Sum of values'
        >>beam.CombineValues(sum)
    )  
    # un-comment to see the results
    # total_products|"total products">>beam.Map(print)


    total_products =(
        products
        |'Sum of values : lambda fn'
        >>beam.CombineValues(lambda val: sum(val))
    )  
    # un-comment to see the results
    # total_products|"print total products">>beam.Map(print)


    total_products =(
        products
        |'Sum of values : lambda fn , multiple args'
        >>beam.CombineValues(lambda val, threshold: min(threshold, sum(val)), threshold=5)
    )  
    # un-comment to see the results
    # total_products|"multiple args">>beam.Map(print)


    
