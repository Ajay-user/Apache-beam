import apache_beam as beam

# Aggregates all input elements by their key and allows downstream processing
# to consume all values associated with the key.

# While GroupByKey performs this operation over a single input collection
# and thus a single type of input values,

# CoGroupByKey operates over multiple input collections.
# 
# As a result, the result for each key is a tuple of the values
# associated with that key in each input collection.


with beam.Pipeline('DirectRunner') as pipe:

    icon_pairs = pipe | 'Create icons' >> beam.Create([
      ('Apple', '🍎'),
      ('Apple', '🍏'),
      ('Eggplant', '🍆'),
      ('Tomato', '🍅'),
    ])

    duration_pairs = pipe | 'Create durations' >> beam.Create([
      ('Apple', 'perennial'),
      ('Carrot', 'biennial'),
      ('Tomato', 'perennial'),
      ('Tomato', 'annual'),
    ])

#  PCollection of (key, value) tuples whose keys are the unique input keys from all inputs,
#  and whose values are dicts mapping each tag to an iterable of whatever values
#  were under the key in the corresponding PCollection, in this manner:

# ('some key', {'tag1': ['value 1 under "some key" in pcoll1',
#                        'value 2 under "some key" in pcoll1',
#                        ...],
#               'tag2': ... ,
#               ... })

    icon_duration = (({'icons':icon_pairs, 'duration':duration_pairs}))

    group = (
        icon_duration
        |"to join both PCollections using their keys"
        >>beam.CoGroupByKey()
        |"print"
        >>beam.Map(print)
    )


  # output
# ('Apple', {'icons': ['🍎', '🍏'], 'duration': ['perennial']})
# ('Eggplant', {'icons': ['🍆'], 'duration': []})
# ('Tomato', {'icons': ['🍅'], 'duration': ['perennial', 'annual']})
# ('Carrot', {'icons': [], 'duration': ['biennial']})