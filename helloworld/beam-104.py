
import apache_beam as beam


# ToString
# Transforms every element in an input collection to a string.
# Any non-string element can be converted to a string using standard Python functions and methods. 
# Many I/O transforms, such as textio.WriteToText, expect their input elements to be strings.



if __name__ == "__main__":

    with beam.Pipeline(runner="DirectRunner") as pipe:

        garden =        (
            pipe | 
            "create a garden" >> beam.Create([
                ('🍓', 'Strawberry'),
                ('🥕', 'Carrot'),
                ('🍆', 'Eggplant'),
                ('🍅', 'Tomato'),
                ('🥔', 'Potato'),
            ])
        )

        
        # Transforms each item in the iterable of the input of PCollection to a string.
        #  There is no trailing delimiter.
        kvs = (
            garden 
            | "Kvs - transform each item in the iterable of the input of PCollection to a string"
            >>beam.ToString.Kvs()
        )

        # Uncomment to see the output
        kvs| "print key values">>beam.Map(print)

        # Transforms each element of the PCollection to a string.
        el = (
            garden
            | "transform each element of the PCollection to a string"
            >>beam.ToString.Element()           
        )

        # Uncomment to see the output
        # el | "print element">>beam.Map(print)


        # Transforms each item in the iterable of the input of PCollection to a string.
        #  There is no trailing delimiter.
        iterable = (
            garden
            | "Iterables - transform each item in the iterable of the input of PCollection to a string"
            >>beam.ToString.Iterables()
        )

        # Uncomment to see the output
        # iterable | "print iterable">>beam.Map(print)


    # Kvswap :
    #  Takes a collection of key-value pairs and returns a collection of key-value pairs
    #  which has each key and value swapped.

        swap = (
            garden
            | "swap key and val"
            >>beam.KvSwap()
        )

        # Uncomment to see the output
        # swap | "print swapped tuples">>beam.Map(print)