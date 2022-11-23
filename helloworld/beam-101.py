import apache_beam as beam


# Map, FlatMap and Filter:

# Map:
# •	Applies a simple 1-to-1 mapping function over each element in the collection.

# FlatMap:
# •	Applies a simple 1-to-many mapping function over each element in the collection.
#   The many elements are flattened into the resulting collection.

# Filter:
# •	Given a predicate, filter out all elements that don’t satisfy that predicate.
# •  May also be used to filter based on an inequality with a given value
#    based on the comparison ordering of the element.

# Lambda:
# •	A lambda function is a small anonymous function.
# •	lambda arguments : expression
# •	x = lambda a : a + 10


if __name__ == "__main__":

    with beam.Pipeline(runner='DirectRunner') as pipe:
        hello = (
            pipe | "Creating Garden : Initializes a Create transform.">>beam.Create([
                    {
                        'icon': '🍓', 'name': 'Strawberry', 'duration': 'perennial'
                    },
                    {
                        'icon': '🥕', 'name': 'Carrot', 'duration': 'biennial'
                    },
                    {
                        'icon': '🍆', 'name': 'Eggplant', 'duration': 'perennial'
                    },
                    {
                        'icon': '🍅', 'name': 'Tomato', 'duration': 'annual'
                    },
                    {
                        'icon': '🥔', 'name': 'Potato', 'duration': 'perennial'
                    },
            ])
                | "filtering perennial plants" >> beam.Filter(lambda x:x['duration']=='perennial')
                | "print the output">>beam.Map(print)

        )