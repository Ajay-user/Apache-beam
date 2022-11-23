
import pathlib
import apache_beam as beam

# ParDo, Keys, KVSwap, ToString and Values.


# ParDo:
# ● ParDo is a Beam transform for generic parallel processing.

# ● The ParDo processing paradigm is similar to the “Map” phase of 
#   a Map/Shuffle/Reduce-style algorithm:

# ● A ParDo transform considers each element in the input PCollection,
#   performs some processing function (your user code) on that element,
#   and emits zero, one, or multiple elements to an output PCollection.

# ● When apply a ParDo transform, need to provide user code in the form of a DoFn object. 


# Dofn:
# ● DoFn is a Beam SDK class that defines a distributed processing function.

# ● The DoFn object that you pass to ParDo contains the processing logic
#   that gets applied to the elements in the input collection. 


# Keys:
# ● Takes a collection of key-value pairs and returns the key to each element.

# Values:
# ● Takes a collection of key-value pairs and returns the value of each element.

# ToString:
# ● Transforms every element in an input collection to a string. 

# Kvswap:
# ● Takes a collection of key-value pairs and returns a collection of key-value pairs
#   which has each key and value swapped.



# When apply a ParDo transform, need to provide user code in the form of a DoFn object. 
class get_name_length(beam.DoFn):
    def process(self, element):
        fields = element.split(',')
        name = fields[1]
        length_of_name = len(name)
        yield (name, length_of_name)


# Check the directory and path of file
data_path = pathlib.Path('./dataset/beam-101/student.txt')
output_path = pathlib.Path('./outputs/helloworld/nameLength')



if __name__ == "__main__":

    with beam.Pipeline(runner='DirectRunner') as pipe:

        student = (
            pipe | "read from text">>beam.io.ReadFromText(str(data_path), skip_header_lines=True)
                 | "parallel do">>beam.ParDo(get_name_length())
                #  | "print">>beam.Map(print)
        )

        student_name = (
            student | "get keys">> beam.Keys()
                    | "print keys">>beam.Map(print)
        )

        student_name_length = (
            student | "get value">>beam.Values()
                    | "print value">>beam.Map(print)
        )



    # with beam.Pipeline(runner='DirectRunner') as pipe:

    #     student = (
    #         pipe | "read from text">>beam.io.ReadFromText(str(data_path),skip_header_lines=1)
    #              | "split">>beam.Map(lambda x: x.split(','))
    #              | "key-val">>beam.Map(lambda x: (x[1],len(x[1])))
    #     )

    #     student_name = (
    #         student | "get keys">> beam.Keys()
    #                 | "print keys">>beam.Map(print)
    #     )

    #     student_name_length = (
    #         student | "get value">>beam.Values()
    #                 | "print value">>beam.Map(print)
    #     )



