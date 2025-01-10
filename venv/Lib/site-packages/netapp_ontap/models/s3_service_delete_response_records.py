r"""
Copyright &copy; 2023 NetApp Inc.
All rights reserved.

This file has been automatically generated based on the ONTAP REST API documentation.

"""

from marshmallow import EXCLUDE, fields  # type: ignore
from netapp_ontap.resource import Resource, ResourceSchema, ResourceSchemaMeta, ImpreciseDateTime, Size


__all__ = ["S3ServiceDeleteResponseRecords", "S3ServiceDeleteResponseRecordsSchema"]
__pdoc__ = {
    "S3ServiceDeleteResponseRecordsSchema.resource": False,
    "S3ServiceDeleteResponseRecordsSchema.opts": False,
    "S3ServiceDeleteResponseRecords": False,
}


class S3ServiceDeleteResponseRecordsSchema(ResourceSchema, metaclass=ResourceSchemaMeta):
    """The fields of the S3ServiceDeleteResponseRecords object"""

    job = fields.Nested("netapp_ontap.models.job_link.JobLinkSchema", unknown=EXCLUDE, data_key="job")
    r""" The job field of the s3_service_delete_response_records. """

    @property
    def resource(self):
        return S3ServiceDeleteResponseRecords

    gettable_fields = [
        "job",
    ]
    """job,"""

    patchable_fields = [
        "job",
    ]
    """job,"""

    postable_fields = [
        "job",
    ]
    """job,"""


class S3ServiceDeleteResponseRecords(Resource):

    _schema = S3ServiceDeleteResponseRecordsSchema
