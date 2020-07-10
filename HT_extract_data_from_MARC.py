import pandas as pd
import pymarc
from pymarc import Record, Field
from pymarc import MARCReader
from tqdm import tqdm
import numpy as np

def extract_mono_records(MARCfile,headerrow):

    with open(MARCfile, 'rb') as f:

        reader = MARCReader(f)
        results = []

        for record in tqdm(reader, desc="Progress", unit=" records"):
            #strip extra values out of the 001 field to get just the MMS_ID (local unique system identifier)
            mms_id = str(record['001'])
            mms_id = mms_id.replace("=001  ","")
            #print(mms_id)

            OCN = ""

            #OCN = OCLC Control Number; Look for 035$a with prefix (OCoLC)
            if record['035']:
                f035 = record.get_fields('035')[0]
                if f035.get_subfields('a'):
                    subfa = f035.get_subfields('a')[0]
                    if 'OCoLC' in subfa:
                        OCN = subfa
                        #print(OCN)
            else:
                # if there's no 035$a with (OCoLC), leave it blank
                OCN = ""


            barcode = ''
            material_type = ''
            description = ''
            perm_lib = ''
            perm_loc = ''
            curr_loc = ''
            process_type = ''
            internal_note1 = ''
            gov_doc_flag = ''

            '''
            Barcode subfield = d
            Material type subfield = e
            Description subfield = f
            Permanent library subfield = b
            Permanent location subfield = c
            Current location subfield = g
            Process type subfield = h
            Internal note 1 subfield = i
            '''

            if record['952']:
                for f952 in record.get_fields('952'):
                    if f952['d']:
                        barcode = f952['d']
                    if f952['e']:
                        material_type = f952['e']
                    if f952['f']:
                        description = f952['f']
                    if f952['b']:
                        perm_lib = f952['b']
                    if f952['c']:
                        perm_loc = f952['c']
                    if f952['g']:
                        curr_loc = f952['g']
                    if f952['h']:
                        process_type = f952['h']
                    if f952['i']:
                        internal_note1 = f952['i']

            if record['951']['f']:
                gov_doc_flag = record['951']['f']


            # compile all the values you just extracted into a row to be appended onto a dataframe
            row = [mms_id, OCN, barcode, material_type, description, perm_lib, perm_loc, curr_loc, process_type,
                   internal_note1, gov_doc_flag]

            # create a mini dataframe of the row you just created and append it to the results list
            df2 = pd.DataFrame([row], columns=headerrow)
            results.append(df2)

    return pd.concat(results)

def extract_analytic_records(MARCfile,headerrow):

    with open(MARCfile, 'rb') as f:

        reader = MARCReader(f)
        results = []

        for record in tqdm(reader, desc="Progress", unit=" records"):
            #strip extra values out of the 001 field to get just the MMS_ID (local unique system identifier)
            mms_id = str(record['001'])
            mms_id = mms_id.replace("=001  ","")
            #print(mms_id)

            OCN = ""

            #OCN = OCLC Control Number; Look for 035$a with prefix (OCoLC)
            if record['035']:
                f035 = record.get_fields('035')[0]
                if f035.get_subfields('a'):
                    subfa = f035.get_subfields('a')[0]
                    if 'OCoLC' in subfa:
                        OCN = subfa
                        #print(OCN)
            else:
                # if there's no 035$a with (OCoLC), leave it blank
                OCN = ""


            barcode = ''
            holdings_id = ''
            material_type = ''
            perm_lib = ''
            perm_loc = ''
            gov_doc_flag = ''

            '''
            Barcode subfield = a
            Holdings ID subfield = 8
            Material type subfield = e
            Permanent library subfield = b
            Permanent location subfield = c
            '''

            if record['952']:
                for f952 in record.get_fields('952'):
                    if f952['a']:
                        barcode = f952['a']
                    if f952['b']:
                        perm_lib = f952['b']
                    if f952['c']:
                        perm_loc = f952['c']
                    if f952['e']:
                        material_type = f952['e']
                    if f952['8']:
                        holdings_id = f952['8']

            if record['951']['f']:
                gov_doc_flag = record['951']['f']


            # compile all the values you just extracted into a row to be appended onto a dataframe
            row = [mms_id, OCN, material_type, perm_lib, perm_loc,gov_doc_flag, holdings_id, barcode]

            # create a mini dataframe of the row you just created and append it to the results list
            df2 = pd.DataFrame([row], columns=headerrow)
            results.append(df2)

    return pd.concat(results)

def extract_ser_records(MARCfile, headerrow):

    with open(MARCfile, 'rb') as f:

        reader = MARCReader(f)
        results = []

        for record in tqdm(reader, desc="Progress", unit=" records"):
            #strip extra values out of the 001 field to get just the MMS_ID (local unique system identifier)
            mms_id = str(record['001'])
            mms_id = mms_id.replace("=001  ","")
            #print(mms_id)

            OCN = ""

            #OCN = OCLC Control Number; Look for 035$a with prefix (OCoLC)
            if record['035']:
                f035 = record.get_fields('035')[0]
                if f035.get_subfields('a'):
                    subfa = f035.get_subfields('a')[0]
                    if 'OCoLC' in subfa:
                        OCN = subfa
                        #print(OCN)
            else:
                # if there's no 035$a with (OCoLC), leave it blank
                OCN = ""

            ISSN = ""
            if record['022']:
                f022 = record.get_fields('022')[0]
                if f022.get_subfields('a'):
                    subfa = f022.get_subfields('a')[0]
                    ISSN = subfa
            else:
                ISSN = ""

            gov_doc_flag = ''

            if record['951']['f']:
                gov_doc_flag = record['951']['f']

            hold_lib = ''
            hold_loc = ''

            '''
            Holdings library subfield = a
            Holdings location subfield = b
            '''

            if record['952']:
                for f952 in record.get_fields('952'):
                    if f952['b']:
                        hold_lib = f952['b']
                    if f952['c']:
                        hold_loc = f952['c']

            # compile all the values you just extracted into a row
            row = [mms_id, OCN, ISSN, hold_lib, hold_loc, gov_doc_flag]

            # create a mini dataframe of the row you just created and append it to the results list
            df2 = pd.DataFrame([row], columns=headerrow)
            results.append(df2)

    return pd.concat(results)


def main():
    ser_q = input('Extract serials file y/n?')
    if ser_q.lower() == 'y':
        serfile = input('Serials MARC file: ')
        ser_headerrow = ['MMS ID', 'OCN','ISSN', 'hold_lib', 'hold_loc', 'gov_doc_flag']
        print(f'Extracting dataframe from {serfile}. Please stand by.')
        ser_df = extract_ser_records(serfile, ser_headerrow)
        serrows, sercols = ser_df.shape
        print(f'Dataframe shape: {serrows} rows, {sercols} columns.')
        ser_df.to_pickle('ser_df.pkl')

    anl_q = input('Extract serial analytics file y/n?')
    if anl_q.lower() == 'y':
        anlfile = input('Serial analytics MARC file: ')
        anl_headerrow = ['MMS ID', 'OCN', 'material type', 'perm_lib','perm_loc', 'gov_doc_flag', 'holdings_id', 'barcode']
        print(f'Extracting dataframe from {anlfile}. Please stand by.')
        anl_df = extract_analytic_records(anlfile, anl_headerrow)
        anlrows, anlcols = anl_df.shape
        print(f'Dataframe shape: {anlrows} rows, {anlcols} columns.')
        anl_df.to_pickle('anl_df.pkl')

    mpm_q = input('Extract multipart monograph file y/n?')
    if mpm_q.lower() == 'y':
        mpmfile = input('Multipart monograph MARC file: ')
        mpm_headerrow = ['MMS ID', 'OCN','barcode','material type','description', 'perm_lib','perm_loc','curr_loc','process_type', 'internal_note1', 'gov_doc_flag']
        print(f'Extracting dataframe from {mpmfile}. Please stand by.')
        mpm_df = extract_mono_records(mpmfile, mpm_headerrow)
        mpmrows, mpmcols = mpm_df.shape
        print(f'Dataframe shape: {mpmrows} rows, {mpmcols} columns.')
        mpm_df.to_pickle('mpm_df.pkl')

    spm_q = input('Extract single part monograph file y/n?')
    if spm_q.lower() == 'y':
        spmfile = input('Single part monograph MARC file: ')
        spm_headerrow = ['MMS ID', 'OCN','barcode','material type','description', 'perm_lib','perm_loc','curr_loc','process_type', 'internal_note1', 'gov_doc_flag']
        print(f'Extracting dataframe from {spmfile}. Please stand by.')
        spm_df = extract_mono_records(spmfile, spm_headerrow)
        spmrows, spmcols = spm_df.shape
        print(f'Dataframe shape: {spmrows} rows, {spmcols} columns.')
        spm_df.to_pickle('spm_df.pkl')

    print('Extracts complete. Thank you and have a great HathiTrust day!')

if __name__ == "__main__":
    main()
