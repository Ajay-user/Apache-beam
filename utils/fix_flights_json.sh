echo "json file fix"

sed --i 's/{.*}/\0,/' flights.json

sed --i 's/.*/[\0]/' flights.json