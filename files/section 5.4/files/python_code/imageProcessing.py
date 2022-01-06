import logging
import apache_beam as beam
from apache_beam.io import ReadFromText
from apache_beam.io.gcp.gcsio import GcsIO
from apache_beam.io import filesystems
from apache_beam.io import WriteToText
from apache_beam.options.pipeline_options import PipelineOptions
from apache_beam.options.pipeline_options import SetupOptions
import numpy as np
import os
import io

from PIL import Image


class ImageThumbnailOptions(PipelineOptions):
    @classmethod
    def _add_argparse_args(cls, parser):
        parser.add_value_provider_argument(
            '--inputBucket',
            help='Path of the file to read from')
        parser.add_value_provider_argument(
            '--outputBucket',
            help='Output file to write results to.')


logging.getLogger().setLevel(logging.INFO)

pipeline_options = PipelineOptions()
pipeline_options.view_as(SetupOptions).save_main_session = True

with beam.Pipeline(options=pipeline_options) as p:
    def print_row(element):
        logging.info("Running pipeline:")


    def loadAndResize(path):
        logging.info("Input:" + str(path))
        buf = GcsIO().open(str(path), mime_type="image/jpeg")
        img = Image.open(io.BytesIO(buf.read()))
        resizedImg = img.resize((150, 150))
        b = io.BytesIO()
        resizedImg.save(b, format='JPEG')
        return {"path": path, "image": b.getvalue()}


    def store(item):
        output_path = my_options.outputBucket.get()
        name = item["path"].split("/")[-1].split(".")[0]
        path = os.path.join(output_path, f"{name}_thumbnail.jpg")
        writer = filesystems.FileSystems.create(path)
        writer.write(item['image'])
        writer.close()


    my_options = pipeline_options.view_as(ImageThumbnailOptions)

    (p
     | "Start" >> beam.Create([""])
     | "ReadInputParam" >> beam.Map(lambda x: my_options.inputBucket.get())
     | "LoadAndResize" >> beam.Map(loadAndResize)
     | 'Write' >> beam.ParDo(store))


    p.run().wait_until_finish()
