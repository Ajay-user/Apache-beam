import csv
import pathlib
from statistics import mean
import apache_beam as beam

# Aggregation
# Grouping is often used in conjunction with aggregation,
# and the aggregate_field method of the GroupBy transform can be used to accomplish this easily.

# This method takes three parameters:
#  the field (or expression) which to aggregate,
#  the CombineFn with which to aggregate by,
#  and finally a field name in which to store the result.
# 





if __name__ == "__main__":

    with beam.Pipeline(runner='DirectRunner') as pipe:

        GROCERY_LIST = [
            beam.Row(recipe='pie', fruit='strawberry', quantity=3, unit_price=1.50),
            beam.Row(recipe='pie', fruit='raspberry', quantity=1, unit_price=3.50),
            beam.Row(recipe='pie', fruit='blackberry', quantity=1, unit_price=4.00),
            beam.Row(recipe='pie', fruit='blueberry', quantity=1, unit_price=2.00),
            beam.Row(recipe='muffin', fruit='blueberry', quantity=2, unit_price=2.00),
            beam.Row(recipe='muffin', fruit='banana', quantity=3, unit_price=1.00),
        ]


        grocery = (
            pipe
            |"create grocery"
            >>beam.Create(GROCERY_LIST)
        )

        #  For example, suppose one wanted to compute the amount of each fruit to buy.

        quantity = (
            grocery
            |"compute the amount of each fruit to buy"
            >>beam.GroupBy('fruit')
            .aggregate_field('quantity', sum, 'total_quantity')
        )

        # uncomment to see the output
        # quantity | "quantity" >> beam.Map(print)      

        # one can also aggregate multiple fields and by expressions

        price = (
            grocery
            |"aggregate multiple fields and by expressions"
            >>beam.GroupBy('recipe')
            .aggregate_field('quantity', sum, 'total_quantity')
            .aggregate_field(lambda x : x.quantity*x.unit_price, sum, 'total_price')
        )
        
          
        # uncomment to see the output
        # price | "price" >> beam.Map(print) 

        # One can, of course, aggregate the same field multiple times as well.
        
        price_composite = (
            grocery
            |"aggregate the same field multiple times"
            >>beam.GroupBy('recipe')
            .aggregate_field('quantity', sum, 'total_quantity')
            .aggregate_field(lambda x : x.quantity*x.unit_price, sum, 'total_price')
            .aggregate_field(lambda x : x.quantity*x.unit_price, min, 'min_price')
            .aggregate_field(lambda x : x.quantity*x.unit_price, max, 'max_price')
        )
        
        # uncomment to see the output
        # price_composite | "price-composite" >>beam.Map(print) 
            

        # illustrates a global grouping,  the grouping key is empty.
        price_global = (
            grocery
            |"global grouping"
            >>beam.GroupBy()
            .aggregate_field(lambda x : x.quantity*x.unit_price, mean, 'mean_price')
            .aggregate_field(lambda x : x.quantity*x.unit_price, min, 'min_price')
            .aggregate_field(lambda x : x.quantity*x.unit_price, max, 'max_price')
        )
        
        # uncomment to see the output
        price_global | "price-global" >>beam.Map(print) 

