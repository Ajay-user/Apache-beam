import apache_beam as beam


class CountPlants(beam.CombineFn):
    def __init__(self, *unused_args, **unused_kwargs):
        super().__init__(*unused_args, **unused_kwargs)
    
    def create_accumulator(self, *args, **kwargs):
        return 0
    
    def add_input(self, mutable_accumulator, element, *args, **kwargs):
        return mutable_accumulator + 1
    
    def merge_accumulators(self, accumulators, *args, **kwargs):
        return sum(accumulators)
    
    def extract_output(self, accumulator, *args, **kwargs):
        return accumulator


with beam.Pipeline() as pipeline:

    plants = (
        pipeline
        | 'Create plants'
        >> beam.Create(['🍓', '🥕', '🥕', '🥕', '🍆', '🍆', '🍅', '🍅', '🍅', '🌽'])
    )

    total = (
        plants
        | ' Count all elements in a PCollection, even if there are duplicate elements.'
        >> beam.combiners.Count.Globally()
        | ' We use Count.Globally() to count all elements in a PCollection '
        >> beam.Map(print)
    )


    # code below uses beam.CombineGlobally to count 
    # this is totaly unnecessary here ,use beam.combiners.Count.Globally()
    # this just for a demo

    combine_globally  = (
        plants
        | 'Use beam.CombineGlobally to count '
        >> beam.CombineGlobally(CountPlants())
        | 'print the count'
        >> beam.Map(print)
    )



