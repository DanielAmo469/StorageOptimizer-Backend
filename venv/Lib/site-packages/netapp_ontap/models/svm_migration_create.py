r"""
Copyright &copy; 2023 NetApp Inc.
All rights reserved.

This file has been automatically generated based on the ONTAP REST API documentation.

"""

from marshmallow import EXCLUDE, fields  # type: ignore
from netapp_ontap.resource import Resource, ResourceSchema, ResourceSchemaMeta, ImpreciseDateTime, Size


__all__ = ["SvmMigrationCreate", "SvmMigrationCreateSchema"]
__pdoc__ = {
    "SvmMigrationCreateSchema.resource": False,
    "SvmMigrationCreateSchema.opts": False,
    "SvmMigrationCreate": False,
}


class SvmMigrationCreateSchema(ResourceSchema, metaclass=ResourceSchemaMeta):
    """The fields of the SvmMigrationCreate object"""

    job = fields.Nested("netapp_ontap.models.job_link.JobLinkSchema", unknown=EXCLUDE, data_key="job")
    r""" The job field of the svm_migration_create. """

    num_records = Size(data_key="num_records")
    r""" Number of records

Example: 1 """

    records = fields.List(fields.Nested("netapp_ontap.resources.svm_migration.SvmMigrationSchema", unknown=EXCLUDE), data_key="records")
    r""" The records field of the svm_migration_create. """

    @property
    def resource(self):
        return SvmMigrationCreate

    gettable_fields = [
        "job",
        "num_records",
        "records",
    ]
    """job,num_records,records,"""

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


class SvmMigrationCreate(Resource):

    _schema = SvmMigrationCreateSchema
