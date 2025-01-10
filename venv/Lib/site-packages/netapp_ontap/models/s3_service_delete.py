r"""
Copyright &copy; 2023 NetApp Inc.
All rights reserved.

This file has been automatically generated based on the ONTAP REST API documentation.

"""

from marshmallow import EXCLUDE, fields  # type: ignore
from netapp_ontap.resource import Resource, ResourceSchema, ResourceSchemaMeta, ImpreciseDateTime, Size


__all__ = ["S3ServiceDelete", "S3ServiceDeleteSchema"]
__pdoc__ = {
    "S3ServiceDeleteSchema.resource": False,
    "S3ServiceDeleteSchema.opts": False,
    "S3ServiceDelete": False,
}


class S3ServiceDeleteSchema(ResourceSchema, metaclass=ResourceSchemaMeta):
    """The fields of the S3ServiceDelete object"""

    num_records = Size(data_key="num_records")
    r""" Number of Records

Example: 1 """

    records = fields.List(fields.Nested("netapp_ontap.models.s3_service_delete_response_records.S3ServiceDeleteResponseRecordsSchema", unknown=EXCLUDE), data_key="records")
    r""" The records field of the s3_service_delete. """

    @property
    def resource(self):
        return S3ServiceDelete

    gettable_fields = [
        "num_records",
        "records",
    ]
    """num_records,records,"""

    patchable_fields = [
        "num_records",
        "records",
    ]
    """num_records,records,"""

    postable_fields = [
        "num_records",
        "records",
    ]
    """num_records,records,"""


class S3ServiceDelete(Resource):

    _schema = S3ServiceDeleteSchema
