'''
 Luigi POC <ben@simulmedia.com>
'''
import luigi, luigi.hdfs, luigi.hadoop
import shutil

class PrepareAMRLD(luigi.ExternalTask):
   def output(self):
      shutil.copy('data/test.raw', 'data/ndh_test.process')
      return luigi.hdfs.HdfsTarget('data/ndh_test.process')

class Streams(luigi.Task):
    def output(self):
        return luigi.hdfs.HdfsTarget('data/ndh_test.process')

    def requires(self):
         return [PrepareAMRLD()]

class ProcessRawAMRLD(luigi.hadoop.JobTask):
    '''
    First process to run
    '''
    def output(self):
        return luigi.hdfs.HdfsTarget(
            "data/r30r36.tsv"
        )

    def mapper(self, line):
        yield line

    def reducer(self, key, values):
        yield key, values

    def requires(self):
         return [Streams()]

# class AMRLDJobTask(luigi.hadoop.JobTask):
#     def internal_reader(self, input_stream):
#         for input in input_stream:
#             yield map(eval, input.split("\n"))

if __name__ == "__main__":
    luigi.run()
