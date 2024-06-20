import csv
import json
import time
from crossref.restful import Works
import sqlite3
from tqdm import tqdm


def get_reference_count_and_indexed_time(doi, timeout=3):
    works = Works()
    response = works.doi(doi)
    count = response['is-referenced-by-count']
    indexed_time = response['indexed']['date-time']
    time.sleep(timeout)
    return count, indexed_time


if __name__ == '__main__':
    conn = sqlite3.connect('PRIDE-metadata.db')
    cursor = conn.cursor()

    cursor.execute('SELECT "accession","references" FROM metadata WHERE "references" != "[]"')
    results = cursor.fetchall()

    with open('PRIDE-referenced-by-count.csv', mode='w', newline='') as csv_file:
        csv_writer = csv.writer(csv_file)
        csv_writer.writerow(['accession', 'doi', 'referenced_by_count', 'indexed_time'])

        for row in tqdm(results):
            accession = row[0]
            references = json.loads(row[1])
            for reference in references:
                if 'doi' in reference:
                    doi = reference['doi']
                    count, indexed_time = get_reference_count_and_indexed_time(doi)
                    csv_writer.writerow([accession, doi, count, indexed_time])

    conn.close()
