'''
Feed a .mrc file to this script to locate a problem record
in the file. Review data_out.mrc to find problem record;
problem record is the next record in the input file after the 
last record in data_out.mrc.
'''

from pymarc import MARCReader, MARCWriter

fname = input("MARC filename: ")

with open(fname,'rb') as f:
   reader = MARCReader(f)
   output = 'data_out.mrc'

   for rec in reader:
       with open(output, 'ab') as x:

           x.write(rec.as_marc())