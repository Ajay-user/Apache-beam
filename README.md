## Apache Beam
Data Engineering with Dataflow using Apache Beam 


### Apache Beam 101


#### Map, FlatMap and Filter:

##### Map:
 •	Applies a simple 1-to-1 mapping function over each element in the collection.

##### FlatMap:
 •	Applies a simple 1-to-many mapping function over each element in the collection.
    The many elements are flattened into the resulting collection.

##### Filter:
 •	Given a predicate, filter out all elements that don’t satisfy that predicate.
 •  May also be used to filter based on an inequality with a given value
    based on the comparison ordering of the element.

##### Lambda:
 •	A lambda function is a small anonymous function.
 •	lambda arguments : expression
 •	x = lambda a : a + 10
