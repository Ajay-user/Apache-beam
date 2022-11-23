import apache_beam as beam
import pathlib

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


# Make sure the directory exists and path specified is correct.
data_path = pathlib.Path('./dataset/beam-101/grocery.txt')
output_path = pathlib.Path('./outputs/helloworld/grocery')


if __name__ == "__main__":

    with beam.Pipeline(runner='DirectRunner') as pipe:

        (
            pipe | "read from text">>beam.io.ReadFromText(str(data_path),skip_header_lines=1)
                 | "split">>beam.Map(lambda x:x.split(','))
                 | 'filter grocery'>>beam.Filter(lambda x:x[10]=='Grocery Store')
                #  | 'print'>>beam.Map(print)
                 | 'write to file'>>beam.io.WriteToText(str(output_path),file_name_suffix='.txt')
        )


