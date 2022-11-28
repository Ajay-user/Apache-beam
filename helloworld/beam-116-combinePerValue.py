import apache_beam as beam

# Combining with a CombineFn
# The more general way to combine elements,
#  and the most flexible, is with a class that inherits from CombineFn.

# CombineFn.create_accumulator():
#  This creates an empty accumulator.
#  For example, an empty accumulator for a sum would be 0, 
#  while an empty accumulator for a product (multiplication) would be 1.

# CombineFn.add_input(): Called once per element.
#  Takes an accumulator and an input element, combines them and returns the updated accumulator.

# CombineFn.merge_accumulators():
#  Multiple accumulators could be processed in parallel,
#  so this function helps merging them into a single accumulator.

# CombineFn.extract_output(): It allows to do additional calculations before extracting a result.




class CombineProducts(beam.CombineFn):
    def create_accumulator(self, *args, **kwargs):
        return {}

    #  Called once for each value of a key.
    def add_input(self, mutable_accumulator, VALUE):
        if VALUE not in mutable_accumulator:
            mutable_accumulator[VALUE]=0
        mutable_accumulator[VALUE]+=1
        return mutable_accumulator
    
    def merge_accumulators(self, accumulators, *args, **kwargs):
        merged = {}
        for acc in accumulators:
            for k,v in acc.items():
                if k not in merged:
                    merged[k] = 0
                merged[k] += v
        return merged


    def extract_output(self, accumulator, *args, **kwargs):
        total_prod_in_a_season = sum(accumulator.values())
        return {k:round(v/total_prod_in_a_season, 3) for k,v in accumulator.items()}

with beam.Pipeline() as pipeline:

    products = (
      pipeline
      | 'Create produce' >> beam.Create([
          ('spring', ['🥕', '🍅', '🥕', '🍅', '🍆']),
          ('summer', ['🥕', '🍅', '🌽', '🍅', '🍅']),
          ('fall', ['🥕', '🥕', '🍅', '🍅']),
          ('winter', ['🍆', '🍆']),
          
          ('spring', ['🥕', '🍅', '🥕', '🍅', '🍆']),
          ('summer', ['🥕', '🍅', '🌽', '🍅', '🍅']),
          ('fall', ['🥕', '🥕', '🍅', '🍅']),
          ('winter', ['🍆', '🍆']),
      ])
    )

    products_per_season=(
        products
        |"Combines an iterable of values in a keyed collection of elements."
        >>beam.CombineValues(CombineProducts())
    )

    products_per_season|beam.Map(print)

  