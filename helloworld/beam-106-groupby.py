import csv
import pathlib
import apache_beam as beam

# Grouping Examples

# In the following example, we create a pipeline with a PCollection of fruits.

# We use GroupBy to group all fruits by the first letter of their name.




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

        # In the case that the property one wishes to group by is an attribute,
        # a string may be passed to GroupBy in the place of a callable expression. 
        # For example, suppose I have the following data

        grocery = (
            pipe
            |"create grocery"
            >>beam.Create(GROCERY_LIST)
        )


        grouped = (
            grocery
            |"group by an attribute"
            >>beam.GroupBy('recipe')
        )
        
        # uncomment to see the output
        # grouped | beam.Map(print)

        composite = (
            grocery
            |"group by many attr"
            >>beam.GroupBy('recipe', isberry=lambda f:'berry' in f.fruit)
        )

        # uncomment to see the output
        # composite | beam.Map(print)      
        
          





