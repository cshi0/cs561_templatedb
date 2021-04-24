import argparse
import struct

parser = argparse.ArgumentParser()
parser.add_argument("address", metavar="addr", type=str)

args = parser.parse_args()

# addr = "/mnt/d/files/BU/CS 561/projects/LSM/cs561_templatedb/test_db/LSM_STORAGE_1_1"

with open(args.address, 'rb') as f:
  pos = 0
  content = f.read()
  numTuples = int(struct.unpack("i", content[0:4])[0])
  print(numTuples)
  pos = 4
  while pos < len(content):
    header = content[pos]
    valid = header & 0x80
    dimension = header & 0x7F
    print(valid != 0, dimension, end=" ")
    pos += 1
    print(struct.unpack("i"*(dimension+1), content[pos:pos + 4*(dimension+1)]))
    pos = pos + 4*(dimension+1)