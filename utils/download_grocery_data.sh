

raw='https://raw.githubusercontent.com/vigneshSs-07/Cloud-AI-Analytics/main/Apache%20Beam%20-Python/data/grocery.txt'

path='./dataset/beam-101/grocery.txt'


curl -o $path $raw

echo "file downloaded check" $path
sleep 3s