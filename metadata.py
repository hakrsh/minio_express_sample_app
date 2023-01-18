import names
import sys
import os
import random
import json

if len(sys.argv) != 2:
    print('Usage: python metadata.py data_dir')
    sys.exit(1)
data_dir = sys.argv[1]
metadata = []
# for each file in the data directory, create a dictionary entry with filename, and the name of the person, and the age
for filename in os.listdir(data_dir):
    name = names.get_full_name()
    age = random.randint(18, 99)
    temp = {'path': os.path.join(data_dir, filename), 'name': name, 'age': age}
    metadata.append(temp)

# write the metadata to a file
with open('metadata.json', 'w') as f:
    json.dump(metadata, f)
