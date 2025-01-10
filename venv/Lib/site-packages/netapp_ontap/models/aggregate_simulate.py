r"""
Copyright &copy; 2023 NetApp Inc.
All rights reserved.

This file has been automatically generated based on the ONTAP REST API documentation.

"""

from marshmallow import EXCLUDE, fields  # type: ignore
from netapp_ontap.resource import Resource, ResourceSchema, ResourceSchemaMeta, ImpreciseDateTime, Size


__all__ = ["AggregateSimulate", "AggregateSimulateSchema"]
__pdoc__ = {
    "AggregateSimulateSchema.resource": False,
    "AggregateSimulateSchema.opts": False,
    "AggregateSimulate": False,
}


class AggregateSimulateSchema(ResourceSchema, metaclass=ResourceSchemaMeta):
    """The fields of the AggregateSimulate object"""

    job = fields.Nested("netapp_ontap.models.job_link.JobLinkSchema", unknown=EXCLUDE, data_key="job")
    r""" The job field of the aggregate_simulate. """

    num_records = Size(data_key="num_records")
    r""" Number of records

Example: 1 """

    records = fields.List(fields.Nested("netapp_ontap.resources.aggregate.AggregateSchema", unknown=EXCLUDE), data_key="records")
    r""" The records field of the aggregate_simulate. """

    warnings = fields.List(fields.Nested("netapp_ontap.models.aggregate_warning.AggregateWarningSchema", unknown=EXCLUDE), data_key="warnings")
    r""" List of validation warnings and remediation advice for the aggregate simulate behavior. """

    @property
    def resource(self):
        return AggregateSimulate

    gettable_fields = [
        "job",
        "num_records",
        "records",
        "warnings",
    ]
    """job,num_records,records,warnings,"""

    patchable_fields = [
        "job",
        "num_records",
        "records",
    ]
    """job,num_records,records,"""

    postable_fields = [
        "job",
        "num_records",
        "records",
    ]
    """job,num_records,records,"""


class AggregateSimulate(Resource):

    _schema = AggregateSimulateSchema
