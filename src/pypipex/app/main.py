# pytype: skip-file

# beam-playground:
#   name: WordCountMinimal
#   description: An example that counts words in Shakespeare's works.
#   multifile: false
#   pipeline_options: --output output.txt
#   context_line: 120
#   categories:
#     - IO
#     - Core Transforms
#     - Flatten
#     - Options
#     - Combiners
#     - Quickstart
#   complexity: MEDIUM
#   tags:
#     - count
#     - strings
#     - hellobeam

import argparse
import logging
import re

import apache_beam as beam
from apache_beam.io import ReadFromText
from apache_beam.io import WriteToText
from apache_beam.options.pipeline_options import PipelineOptions
from apache_beam.options.pipeline_options import SetupOptions


def main(argv=None, save_main_session=True):
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
      # CHANGE 1/6: (OPTIONAL) The Google Cloud Storage path is required
      # for outputting the results.
      default='gs://YOUR_OUTPUT_BUCKET/AND_OUTPUT_PREFIX',
      help='Output file to write results to.')

  # If you use DataflowRunner, below options can be passed:
  #   CHANGE 2/6: (OPTIONAL) Change this to DataflowRunner to
  #   run your pipeline on the Google Cloud Dataflow Service.
  #   '--runner=DirectRunner',
  #   CHANGE 3/6: (OPTIONAL) Your project ID is required in order to
  #   run your pipeline on the Google Cloud Dataflow Service.
  #   '--project=SET_YOUR_PROJECT_ID_HERE',
  #   CHANGE 4/6: (OPTIONAL) The Google Cloud region (e.g. us-central1)
  #   is required in order to run your pipeline on the Google Cloud
  #   Dataflow Service.
  #   '--region=SET_REGION_HERE',
  #   CHANGE 5/6: Your Google Cloud Storage path is required for staging local
  #   files.
  #   '--staging_location=gs://YOUR_BUCKET_NAME/AND_STAGING_DIRECTORY',
  #   CHANGE 6/6: Your Google Cloud Storage path is required for temporary
  #   files.
  #   '--temp_location=gs://YOUR_BUCKET_NAME/AND_TEMP_DIRECTORY',
  #   '--job_name=your-wordcount-job',
  known_args, pipeline_args = parser.parse_known_args(argv)
  print(f'known_args:{known_args}')
  print(f'pipeline_args:{pipeline_args}')
  # We use the save_main_session option because one or more DoFn's in this
  # workflow rely on global context (e.g., a module imported at module level).
  pipeline_options = PipelineOptions(pipeline_args)
  pipeline_options.view_as(SetupOptions).save_main_session = save_main_session
  with beam.Pipeline(options=pipeline_options) as p:
    pass


if __name__ == '__main__':
  logging.getLogger().setLevel(logging.INFO)
  main()
