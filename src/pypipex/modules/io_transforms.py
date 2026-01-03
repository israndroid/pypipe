import apache_beam as beam


class IOReadFromText(beam.PTransform):
    """
    A Beam PTransform that applies IO transformations to a PCollection.
    This transform is used to read from a text file and create a PCollection
    """

    def __init__(self, input_bucket, skip_header_lines:str=1):
        super().__init__()
        self.input_bucket = input_bucket
        self.skip_header_lines = skip_header_lines

    def expand(self, pcoll):
        return (
            pcoll
            | 'ReadFromText' >> beam.io.ReadFromText(self.input_bucket, skip_header_lines=self.skip_header_lines)
        )
    
class IOWriteToText(beam.PTransform):
    """
    A Beam PTransform that applies IO transformations to a PCollection.
    This transform is used to write a PCollection to a text file
    """

    def __init__(self, output_bucket:str):
        super().__init__()
        self.output_bucket = output_bucket

    def expand(self, pcoll):
        return (
            pcoll
            | 'WriteToText' >> beam.io.WriteToText(self.output_bucket)
        )