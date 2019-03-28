# -*- coding: utf-8 -*-

import json
import logging

import apache_beam as beam
from apache_beam.io import ReadFromText
from apache_beam.io.gcp.datastore.v1.datastoreio import WriteToDatastore
from apache_beam.options.pipeline_options import PipelineOptions



class EntityWrapper(object):
    """Create a Cloud Datastore entity from the given content."""
    def __init__(self, kind, namespace=None):
        self._namespace = namespace
        self._kind = kind

    def make_entity(self, content):
        import uuid
        from google.cloud.proto.datastore.v1 import entity_pb2
        from googledatastore import helper as datastore_helper

        entity = entity_pb2.Entity()
        if self._namespace.get() is not None:
            entity.key.partition_id.namespace_id = self._namespace.get()

        datastore_helper.add_key_path(entity.key, self._kind.get(), str(uuid.uuid4()))

        datastore_helper.add_properties(entity, content)
        return entity


class SwOptions(PipelineOptions):
    @classmethod
    def _add_argparse_args(cls, parser):
        parser.add_value_provider_argument('--input',
                                           dest='input',
                                           default='../dataset/planets.json',
                                           help='Input file specified a local or GCS path with json data.')
        parser.add_argument('--dataset',
                            dest='dataset',
                            required=True,
                            help='Dataset ID to read from Cloud Datastore.')
        parser.add_value_provider_argument('--kind',
                                           dest='kind',
                                           required=True,
                                           help='Datastore Kind')
        parser.add_value_provider_argument('--namespace',
                                           dest='namespace',
                                           default='sw',
                                           help='Datastore Namespace')


def run(argv=None):

    pipeline_options = PipelineOptions()

    sw_options = pipeline_options.view_as(SwOptions)

    with beam.Pipeline(options=pipeline_options) as p:
        (p
         | 'read' >> ReadFromText(sw_options.input)
            # code
         | 'create entity' >> beam.Map(EntityWrapper(sw_options.kind, namespace=sw_options.namespace).make_entity)
         | 'write' >> WriteToDatastore(sw_options.dataset))


if __name__ == '__main__':
    logging.getLogger().setLevel(logging.INFO)
    run()
