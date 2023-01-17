import apache_beam as beam
import random



with beam.Pipeline('DirectRunner') as pipe:

    numbers = (
        pipe
        | "create random numbers"
        >> beam.Create([random.randint(0,9) for i in range(10)])
    )
    # numbers | 'print random numbers'>>beam.Map(print)

    vegetable_counter = (
        pipe
        | 'create random vegetable-count pairs'
        >>beam.Create([ 
          ('🥕', 3),
          ('🥕', 2),
          ('🍆', 1),
          ('🍅', 4),
          ('🍅', 5),
          ('🍅', 3),
        ])
    )
    # vegetable_counter | 'print vegetable pairs'>>beam.Map(print)


    maximum_number = (
        numbers
        |'get the maximum number'
        >> beam.CombineGlobally(max)
    )
    # maximum_number | 'print maximum_number'>>beam.Map(print)

    minimum_number = (
        numbers
        |'get the minimum number'
        >> beam.CombineGlobally(min)
    )
    # minimum_number | 'print minimum_number'>>beam.Map(print)

    sum_of_numbers = (
        numbers
        |'get the sum of number'
        >> beam.CombineGlobally(sum)
    )
    # sum_of_numbers | 'print sum_of_numbers'>>beam.Map(print)

    sum_of_veg_per_key = (
        vegetable_counter
        |'get the sum veg per key'
        >> beam.CombinePerKey(sum)
    )
    # sum_of_veg_per_key | 'print sum_of_veg_per_key'>>beam.Map(print)

    maximum_number_per_key = (
        vegetable_counter
        | ' Maximum elements for each key '
        >> beam.CombinePerKey(max)
    )
    # maximum_number_per_key | 'print maximum_number_per_key'>>beam.Map(print)


    minimum_number_per_key = (
        vegetable_counter
        | ' Minimum elements for each key '
        >> beam.CombinePerKey(min)
    )
    # minimum_number_per_key | 'print minimum_number_per_key'>>beam.Map(print)

    
    global_mean_of_numbers = (
        numbers
        |'get the global mean of number'
        >> beam.combiners.Mean.Globally()
    )
    # global_mean_of_numbers | 'print global_mean_of_numbers'>>beam.Map(print)


    mean_of_number_of_veg = (
        vegetable_counter
        |'get the mean number of veg per key'
        >> beam.combiners.Mean.PerKey()
    )
    # mean_of_number_of_veg | 'print mean_of_number_of_veg'>>beam.Map(print)