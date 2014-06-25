'''
 Luigi POC <ben@simulmedia.com>
'''
import luigi, luigi.hdfs, luigi.hadoop
import shutil

class PrepareAMRLD(luigi.ExternalTask):
   def output(self):
      shutil.copy('data/ndh_test.raw', 'data/ndh_test.process')
      return luigi.LocalTarget('data/ndh_test.process')

class Streams(luigi.Task):
    def output(self):
        return luigi.LocalTarget('data/ndh_test.process')

    def requires(self):
         return [PrepareAMRLD()]

class ProcessRawAMRLD(luigi.hadoop.JobTask):
    '''
    First process to run
    '''
    def output(self):
        return luigi.file.File(
            "data/r30r36.tsv"
        )

    def mapper(self, line):
        print "line: %s" % line
        yield line
        

    def reducer(self, key, values):
        yield key, values

    def requires(self):
         return [Streams()]

if __name__ == "__main__":
    luigi.run()
