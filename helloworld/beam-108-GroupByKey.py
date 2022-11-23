
import apache_beam as beam

# GroupByKey
# Takes a keyed collection of elements and produces a collection 
# where each element consists of a key and all values associated with that key.


with beam.Pipeline("DirectRunner") as pipe:
    fruit=(pipe
        |'Create produce counts'
        >>beam.Create([
            ('spring', '🍓'),
            ('spring', '🥕'),
            ('spring', '🍆'),
            ('spring', '🍅'),
            ('summer', '🥕'),
            ('summer', '🍅'),
            ('summer', '🌽'),
            ('fall', '🥕'),
            ('fall', '🍅'),
            ('winter', '🍆'),])
        |'group by key'
        >>beam.GroupByKey()
        |'print'
        >>beam.Map(print)
        )


