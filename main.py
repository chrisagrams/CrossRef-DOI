import csv
import json
import time
from crossref.restful import Works
import sqlite3
from tqdm import tqdm
import requests


def get_reference_count_and_indexed_time(doi, timeout=3, retries=3):
    works = Works()
    for attempt in range(retries):
        try:
            response = works.doi(doi)
            if response is None:
                raise requests.exceptions.ConnectionError
            count = response['is-referenced-by-count']
            indexed_time = response['indexed']['date-time']
            time.sleep(timeout)
            return count, indexed_time
        except requests.exceptions.ConnectTimeout as e:
            if attempt < retries - 1:
                print(f"Retry {attempt + 1}/{retries} for DOI {doi} due to timeout.")
                time.sleep(2 ** attempt)  # Exponential backoff
            else:
                print(f"Failed to get data for DOI {doi} after {retries} retries.")
                return None, None


def load_existing_data(filename):
    data = {}
    try:
        with open(filename, mode='r') as csv_file:
            csv_reader = csv.reader(csv_file)
            next(csv_reader)  # Skip the header
            for row in csv_reader:
                accession, doi, count, indexed_time = row
                data[(accession, doi)] = (count, indexed_time)
    except FileNotFoundError:
        pass
    return data


if __name__ == '__main__':
    conn = sqlite3.connect('PRIDE-metadata.db')
    cursor = conn.cursor()

    cursor.execute('SELECT "accession","references" FROM metadata WHERE "references" != "[]"')
    results = cursor.fetchall()

    existing_data = load_existing_data('PRIDE-referenced-by-count.csv')

    with open('PRIDE-referenced-by-count.csv', mode='a', newline='') as csv_file:
        csv_writer = csv.writer(csv_file)
        if csv_file.tell() == 0:
            csv_writer.writerow(['accession', 'doi', 'referenced_by_count', 'indexed_time'])

        for row in tqdm(results):
            accession = row[0]
            references = json.loads(row[1])
            for reference in references:
                if 'doi' in reference:
                    doi = reference['doi']
                    if (accession, doi) not in existing_data:
                        count, indexed_time = get_reference_count_and_indexed_time(doi)
                        if count is None:
                            count = ''
                        if indexed_time is None:
                            indexed_time = ''
                        csv_writer.writerow([accession, doi, count, indexed_time])
                    else:
                        print(f"Skipping {doi} for {accession} as it already exists in the CSV.")

    conn.close()
