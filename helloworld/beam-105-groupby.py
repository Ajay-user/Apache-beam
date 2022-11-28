import csv
import pathlib
import apache_beam as beam

# Grouping Examples

# In the following example, we create a pipeline with a PCollection of fruits.

# We use GroupBy to group all fruits by the first letter of their name.




if __name__ == "__main__":

    with beam.Pipeline(runner='DirectRunner') as pipe:

        fruits=(
            pipe |
            "create fruits">>
            beam.Create(['strawberry', 'raspberry', 'blueberry', 'blackberry', 'banana'])
        )

        # We use GroupBy to group all fruits by the first letter of their name.

        first_letter=(
            fruits 
            |"group all fruits by the first letter of their name"
            >>beam.GroupBy(lambda fruit:fruit[0])
        )

        # uncomment to see the output 
        first_letter | "first_letter" >> beam.Map(print)

        
        # We can group by a composite key consisting of multiple properties

        composite=(
            fruits
            |"group by a composite key consisting of multiple properties"
            >>beam.GroupBy(letters=lambda f:f[0], isberry=lambda f: 'berry' in f)
        )

        # uncomment to see the output
        # composite | "composite" >> beam.Map(print)


