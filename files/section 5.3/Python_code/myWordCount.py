import logging
import apache_beam as beam
from apache_beam.io import ReadFromText
from apache_beam.io import WriteToText
from apache_beam.options.pipeline_options import PipelineOptions
        
class WordcountOptions(PipelineOptions):
    @classmethod
    def _add_argparse_args(cls, parser):
      # Use add_value_provider_argument for arguments to be templatable
      # Use add_argument as usual for non-templatable arguments
      parser.add_value_provider_argument(
          '--input1',
          default='gs://dataflow-samples/shakespeare/kinglear.txt',
          help='Path of the file to read from')
      parser.add_value_provider_argument(
          '--input2',
          default='gs://dataflow-samples/shakespeare/romeoandjuliet.txt',
          help='Path of the file to read from')
      parser.add_value_provider_argument(
          '--input3',
          default='gs://dataflow-samples/shakespeare/macbeth.txt',
          help='Path of the file to read from')
      parser.add_value_provider_argument(
          '--output1',
          help='Output file to write results to.')
      parser.add_value_provider_argument(
          '--output2',
          help='Output file to write results to.')
          
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

logging.getLogger().setLevel(logging.INFO)

pipeline_options = PipelineOptions()
# Create pipeline.
with beam.Pipeline(options=pipeline_options) as p:
    def print_row(element):
        logging.info("Running pipeline:")
    my_options = pipeline_options.view_as(WordcountOptions)
    
    # Read the text file[pattern] into a PCollection.
    lines1 = p | 'Read1' >> ReadFromText(my_options.input1)
    lines2 = p | 'Read2' >> ReadFromText(my_options.input2)
    lines3 = p | 'Read3' >> ReadFromText(my_options.input3)
    
    counts = (
        (lines1,lines2,lines3)
        | 'Flatten' >> beam.Flatten()
        | 'Split' >> (beam.ParDo(WordExtractingDoFn()).with_output_types(str))
        | 'PairWIthOne' >> beam.Map(lambda x: (x, 1))
        | 'GroupAndSum' >> beam.CombinePerKey(sum))
    
    # Format the counts into a PCollection of strings.
    def format_result(word, count):
      return '%s: %d' % (word, count)

    output = counts | 'Format' >> beam.MapTuple(format_result)

    # Write the output using a "Write" transform that has side effects.
    # pylint: disable=expression-not-assigned
    output | 'Write1' >> WriteToText(my_options.output1)
    output | 'Write2' >> WriteToText(my_options.output2)
    
    
    p.run().wait_until_finish()
