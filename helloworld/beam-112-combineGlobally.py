
import apache_beam as beam

class CombineProducts(beam.CombineFn):

    def create_accumulator(self,):
        return {}

    def add_input(self, accumulator, input):
        for product in input:
            if product not in accumulator:
                accumulator[product]=0
            accumulator[product]+=1
        return accumulator

        
    def merge_accumulators(self, accumulators):
        merged = {}
        for acc in accumulators:
            for k,v in acc.items():
                if k not in merged:
                    merged[k]=0
                merged[k] += v
        return merged


    def extract_output(self, accumulator):
        total_products = sum(accumulator.values())
        percentage = {k:round(v/total_products,3) for k,v in accumulator.items()}
        return percentage




with beam.Pipeline(runner='directRunner') as pipe:

    products = (
        pipe
        |"create an iterable of products"
        >>beam.Create([ 
          {'🍓', '🥕', '🍌', '🍅', '🌶️'},
          {'🍇', '🥕', '🥝', '🍅', '🥔'},
          {'🍉', '🥕', '🍆', '🍅', '🍍'},
          {'🥑', '🥕', '🌽', '🍅', '🥥'},
        ])

    )

    # Combine the products globally
    # and get the count_of_each_product / total_products

    countPerTotalProducts = (
        products
        |"Combine the list of products to perform some additional calculations"
        >>beam.CombineGlobally(CombineProducts())
        |"Print the output"
        >>beam.Map(print)
    )
