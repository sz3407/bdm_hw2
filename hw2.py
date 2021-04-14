from mrjob.job import MRJob

class MRHW2(MRJob):

  def mapper(self, _, line):
    row = next(csv.reader([line]))
    if len(row) == 18 and len(row[0]) == 10 and row[0].replace('-','').isdigit():     # check 18 columns, length of first element = datetime, replace '-'
      year = row[0][:4]
      product = row[1].lower()
      company = row[7]
      yield ((year, product), company)

  def reducer(self, key, companies):      # generator, cannot iterate twice
    counts = {}
    for c in companies:                   # must have for loop for iteration
      counts[c] = counts.get(c, 0) + 1
    complaints = sum(counts.values())
    numCompanies = len(counts)
    percentage = int(max(counts.values()) * 100 / complaints + 0.5)
    yield (','.join(map(str, [*key, complaints, numCompanies, percentage])), '')

if __name__ == '__main__':
  MRHW2.run()