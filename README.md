## Apache Beam
Data Engineering with Dataflow using Apache Beam 


### Apache Beam 101

Go to `Apache-beam/helloworld/`  to learn the basics then go to `Apache-beam/foundation/`

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




##### ParDo:
 ● ParDo is a Beam transform for generic parallel processing.

 ● The ParDo processing paradigm is similar to the “Map” phase of 
   a Map/Shuffle/Reduce-style algorithm:

 ● A ParDo transform considers each element in the input PCollection,
   performs some processing function (your user code) on that element,
   and emits zero, one, or multiple elements to an output PCollection.

 ● When apply a ParDo transform, need to provide user code in the form of a DoFn object. 


##### Dofn:
 ● DoFn is a Beam SDK class that defines a distributed processing function.

 ● The DoFn object that you pass to ParDo contains the processing logic
   that gets applied to the elements in the input collection. 


##### Keys:
 ● Takes a collection of key-value pairs and returns the key to each element.

##### Values:
 ● Takes a collection of key-value pairs and returns the value of each element.

##### ToString:
 ● Transforms every element in an input collection to a string. 

##### Kvswap:
 ● Takes a collection of key-value pairs and returns a collection of key-value pairs
   which has each key and value swapped.



##### Grouping Examples
######  We use GroupBy to group all fruits by the first letter of their name.


`
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

`

##### GroupByKey
Takes a keyed collection of elements and produces a collection where each element consists of a key and all values associated with that key.

* Aggregates all input elements by their key and allows downstream processing to consume all values associated with the key.
* While GroupByKey performs this operation over a single input collection and thus a single type of input values, CoGroupByKey operates over multiple input collections.

##### CoGroupByKey operates over multiple input collections.

* The result for each key is a tuple of the values associated with that key in each input collection.








