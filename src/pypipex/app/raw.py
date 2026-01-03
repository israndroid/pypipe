import argparse
from datetime import datetime, timedelta
import logging
import time
import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions
from apache_beam.options.pipeline_options import SetupOptions
from src.pypipex.modules.raw_transforms import SplitRawLineToDict
from src.pypipex.modules.io_transforms import IOReadFromText, IOWriteToText
from config import global_config as GC

def setup_options(argv=None, save_main_session=True):
    parser = argparse.ArgumentParser()
    parser.add_argument('--runner', dest='runner', default='DirectRunner', choices=['DirectRunner', 'DataflowRunner'], help='Runner to use for the pipeline')
    parser.add_argument('--env', dest='environment', default='DEV',choices=GC.ENV_OPTIONS, help=f'Environment to run the pipeline ({GC.ENV_DEV}: Local, {GC.ENV_PROD}: pre-prod environment (Example: test at DataFlowRunner), {GC.ENV_PROD}: Production environment)')
    parser.add_argument('--input_bucket', dest='input_bucket', required=True, help='Path to read RAW file')
    parser.add_argument('--output_bucket', dest='output_bucket', required=True, help='Path to write STANDARIZED file') 
    parser.add_argument('--transforms', dest='transforms', default=['IOReadFromText', 'StandarizeHeader', 'IOWriteToText'], help='Raw transform to apply on the input data')
    parser.add_argument('--date_calc', dest='date_calc', default=datetime.now().strftime('%Y-%m-%d'), help='Date to calculate the data (default: yesterday)')

    know_args, pipeline_args = parser.parse_known_args()
    pipeline_options = PipelineOptions(pipeline_args)
    pipeline_options.view_as(SetupOptions).save_main_session = save_main_session
    return know_args, pipeline_options

def main(pipeline_options, args, date_calc):
    if args.environment == GC.ENV_DEV:
        logging.info(f'Running in {GC.ENV_DEV} environment')
    elif args.environment == GC.ENV_UAT:    
        logging.info(f'Running in {GC.ENV_UAT} environment')
    elif args.environment == GC.ENV_PROD:
        logging.info(f'Running in {GC.ENV_PROD} environment')
    
    logging.info(f'Pipeline options parsed: {pipeline_options}')
    logging.info(f'Args parsed: {args}')

    with beam.Pipeline(options=pipeline_options) as p:
        logging.info(f'Transforms to apply: {args.transforms}')
        #pcol_transform_applied = [p]
        """ for transform in args.transforms:
            logging.info(f'Applying transform: {transform}')
            pcol_transform_applied.append(
                pcol_transform_applied[i] | transform(
            ) """
        prefix_step = 'Step 01 - Read, Transform and Write to Text'
        pcol_transform_applied = (p
            | f'{prefix_step} - IOReadFromText' >> IOReadFromText(input_bucket=args.input_bucket)
            #| 'beam.map(split_raw_line_to_dict)' >> beam.Map(lambda row: split_raw_line_to_dict(row, header='Price_CLP,Price_UF,Price_USD,Comuna,Ubicacion,Dorms,Baths,Built Area,Total Area,Parking,id,Realtor', delimiter=','))
            | f'{prefix_step} - SplitRawLineToDict' >> SplitRawLineToDict(
                header='Price_CLP,Price_UF,Price_USD,Comuna,Ubicacion,Dorms,Baths,Built Area,Total Area,Parking,id,Realtor',
                delimiter=',',
                to_sanitize_header=True,
                to_lower_case=True,
                tag='SplitRawLineToDict'
            )
            #| 'SelectRenamedCreateIfNotExists' >> SelectRenamedCreateIfNotExists() #TODO: Implement this transform
            | f'{prefix_step} - IOWriteToText' >> IOWriteToText(output_bucket=args.output_bucket)
        )

    

if __name__ == '__main__':
    logging.getLogger().setLevel(logging.INFO)
    know_args, pipeline_options = setup_options(argv=None, save_main_session=True)

    start_time = time.time()
    print(f'[start_time: {start_time}] Starting the pipeline... raw.py -> main()')
    
    main(pipeline_options=pipeline_options, args=know_args, date_calc=know_args.date_calc)
    end_time = time.time()

    print(f'[end_time: {end_time}] Ending the pipeline... raw.py -> main()')