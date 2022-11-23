
import apache_beam as beam
import csv
import pathlib



# Parses a text file as newline-delimited elements, by default assuming UTF-8 encoding.
#  Supports newline delimiters \n and \r\n or specified delimiter .

# Use the csv.reader object to read the CSV file
# csvreader = csv.reader(file)
# Extract the field names
# Create an empty list called header. Use the next() method to obtain the header.

# The .next() method returns the current row and moves to the next row.

# The first time you run next() it returns the header
#  and the next time you run it returns the first record and so on.

# header = next(csvreader)

# Extract the rows/records
# Create an empty list called rows and iterate through the csvreader object
#  and append each row to the rows list.

# rows = []
# for row in csvreader:
#         rows.append(row)



# import csv
# rows = []
# with open("Salary_Data.csv", 'r) as file:
#     csvreader = csv.reader(file)
#     header = next(csvreader)
#     for row in csvreader:
#         rows.append(row)
# print(header)
# print(rows)



# AIRPORT CSV
# INDEX    field-name 
# 0 	'AIRPORT_SEQ_ID'    
# 21 	'LATITUDE'                        
# 26 	'LONGITUDE' 




if __name__ == '__main__':

    airport_csv_path = pathlib.Path.cwd()/'dataset'/'airports_csv'/'airports.csv'
    airport_ouput_path = pathlib.Path.cwd()/'outputs'/'streaming-example'/'extracted_airports'

    with beam.Pipeline('DirectRunner') as pipe:
        airports = (
            pipe | beam.io.ReadFromText(str(airport_csv_path))
                                        # put the line in a list 
                 | beam.Map(lambda line : next(csv.reader([line]))) # This will output a row [ c1, c2, .. ]
                 | beam.Map(lambda field: (field[0], (field[21], field[26])))
        )

        (
            airports | beam.Map(lambda data: f"{data[0]}, {','.join(data[1])}")
                  | beam.io.WriteToText(str(airport_ouput_path), file_name_suffix='.csv')
        )