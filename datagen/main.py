import sys

sys.path.append('/Users/anarasimham/code/datagen')

from datagen import POSDataGenerator

a = POSDataGenerator()
print(a.gen_row())
