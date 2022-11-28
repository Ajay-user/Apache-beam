

import apache_beam as beam

# CombineGlobally
# CombineGlobally accepts a function that takes an iterable of elements as an input,
# and combines them to return a single element.

with beam.Pipeline('DirectRunner') as pipe:

    produce = (
    pipe 
    | 'Create icons : iterable of sets' 
    >> beam.Create([
        {'🍓', '🥕', '🍌', '🍅', '🌶️'},
        {'🍇', '🥕', '🥝', '🍅', '🥔'},
        {'🍉', '🥕', '🍆', '🍅', '🍍'},
        {'🥑', '🥕', '🌽', '🍅', '🥥'},
    ]))


    combine = (
        produce
        |"combine globally : calculates the intersection"
        >>beam.CombineGlobally(lambda sets : set.intersection(*sets))
        |"print intersection"
        >>beam.Map(print)
    )


# You can pass functions with multiple arguments to CombineGlobally.
# They are passed as additional positional arguments or keyword arguments to the function.
    
    combine_exclude = (
        produce
        |"combine globally with multiple args : calculates the intersection"
        >>beam.CombineGlobally(lambda sets, exclude : set.intersection(*sets)-exclude, exclude={'🥕'})
        |"print intersection after exclusion"
        >>beam.Map(print)
    )


# suppose our data has nothing in common

    new_produce = (
        pipe
        |"create new products that has nothing in common"
        >>beam.Create([{'🥑', '🥕', '🌽'}, {'🍌', '🍅', '🌶️'}]) 
    )

    # lets combine them 
    common_items = (
        new_produce
        |"combine the items and output the intersection"
        >>beam.CombineGlobally(lambda sets: set.intersection(*sets))
        |"lets print the intersection"
        >>beam.Map(print)
    ) 



