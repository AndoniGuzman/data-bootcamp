
#
# Licensed to the Apache Software Foundation (ASF) under one or more
# contributor license agreements.  See the NOTICE file distributed with
# this work for additional information regarding copyright ownership.
# The ASF licenses this file to You under the Apache License, Version 2.0
# (the "License"); you may not use this file except in compliance with
# the License.  You may obtain a copy of the License at
#
#    http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#

"""A word-counting workflow."""

# pytype: skip-file

import argparse
import logging
import re
import csv 
import io
from datetime import datetime
import apache_beam as beam
from apache_beam.io import ReadFromText
from apache_beam.io import WriteToText
from apache_beam.options.pipeline_options import PipelineOptions
from apache_beam.options.pipeline_options import SetupOptions
from apache_beam.transforms.core import Map

class WordExtractingDoFn(beam.DoFn):
  """Parse each line of input text into words."""
  def process(self, element):
    """Returns an iterator over the words of this element.
    The element is a line of text.  If the line is blank, note that, too.
    Args:
      element: the element being processed
    Returns:
      The processed element.
    """
    return re.findall(r'[\w\']+', element, re.UNICODE)

def run(argv=None, save_main_session=True):
  """Main entry point; defines and runs the wordcount pipeline."""
  parser = argparse.ArgumentParser()
  parser.add_argument(
      '--input',
      dest='input',
      default='gs://dataflow-samples/shakespeare/kinglear.txt',
      help='Input file to process.')
  parser.add_argument(
      '--output',
      dest='output',
      required=True,
      help='Output file to write results to.')
  known_args, pipeline_args = parser.parse_known_args(argv)

  # We use the save_main_session option because one or more DoFn's in this
  # workflow rely on global context (e.g., a module imported at module level).
  pipeline_options = PipelineOptions(pipeline_args)
  pipeline_options.view_as(SetupOptions).save_main_session = save_main_session

  with beam.Pipeline(options=pipeline_options) as p:
    
    def searchWord(element):
      dateTimeObj = datetime.now()
      timestampStr = dateTimeObj.strftime("%H%M%S")
      for i in element:  
        if isinstance(i,str) and (' ' + "good" + ' ') in (' ' + i + ' '):
          return [element[0],1,element[0] + "_" +timestampStr]
      return [element[0],0,element[0] + "_" +timestampStr]
        
    def format_result(word_count):
      idUser, review, review_id = word_count
      return '%s, %s, %s' % (idUser, review, review_id)

    #Pipeline
    (p 
    | beam.Create(["movie_review2.csv"])
    |  beam.FlatMap(lambda filename:
        csv.reader(io.TextIOWrapper(beam.io.filesystems.FileSystems.open(known_args.input))))
    | beam.Map(searchWord)
    | beam.Map(format_result)
    | WriteToText(known_args.output)
    )
  
if __name__ == '__main__':
  logging.getLogger().setLevel(logging.INFO)
  run()
