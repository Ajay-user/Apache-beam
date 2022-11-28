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


class ProductPercentage(beam.CombineFn):
    def create_accumulator(self, *args, **kwargs):
        return {}
    
    def add_input(self, mutable_accumulator, element):
        mutable_accumulator[element[0]] = element[1]
        return mutable_accumulator
    
    def merge_accumulators(self, accumulators, *args, **kwargs):
        merged = {}
        for acc in accumulators:
            for k, v in acc.items():
                if k not in merged:
                    merged[k]=0
                merged[k]+=v
        return merged

    def extract_output(self, accumulator, *args, **kwargs):
        total_products = sum(accumulator.values())
        return {k:round(v/total_products, 3) for k,v in accumulator.items()}



####################  COMBINE PER KEY  #################################

class AverageFn(beam.CombineFn):
    def create_accumulator(self, *args, **kwargs):
        sum , count = 0, 0
        return sum, count
    
    #  Called once per key.
    def add_input(self, mutable_accumulator, VALUE):
        sum, count = mutable_accumulator
        return sum+VALUE, count+1
    
    def merge_accumulators(self, accumulators, *args, **kwargs):
        # accumulators = [(sum1, count1), (sum2, count2), (sum3, count3), ...]
        sums, counts = zip(*accumulators)
        # sums = [sum1, sum2, sum3, ...]
        # counts = [count1, count2, count3, ...]
        return sum(sums), sum(counts)

    def extract_output(self, accumulator, *args, **kwargs):
        sum, count = accumulator
        return sum/ count



with beam.Pipeline('DirectRunner') as pipe:

    products=(
        pipe
        |"create some products"
        >>beam.Create([
          ('🥕', 3),
          ('🥕', 2),
          ('🍆', 1),
          ('🍅', 4),
          ('🍅', 5),
          ('🍅', 3),

        ])
    )


    productPercentage = (
        products
        |"combine Globally and calculate percentage"
        >>beam.CombineGlobally(ProductPercentage())
        |"results"
        >>beam.Map(print)
    )

    # AVERAGE COMPUTATION

    productAverage = (
        products
        |"combine per key and calculate mean"
        >>beam.CombinePerKey(AverageFn())
        |"Combines all elements for each key in a collection."
        >>beam.Map(print)
    )