r"""
Copyright &copy; 2023 NetApp Inc.
All rights reserved.

This file has been automatically generated based on the ONTAP REST API documentation.

"""

from marshmallow import EXCLUDE, fields  # type: ignore
from netapp_ontap.resource import Resource, ResourceSchema, ResourceSchemaMeta, ImpreciseDateTime, Size


__all__ = ["MetroclusterSvmPartnerSvm", "MetroclusterSvmPartnerSvmSchema"]
__pdoc__ = {
    "MetroclusterSvmPartnerSvmSchema.resource": False,
    "MetroclusterSvmPartnerSvmSchema.opts": False,
    "MetroclusterSvmPartnerSvm": False,
}


class MetroclusterSvmPartnerSvmSchema(ResourceSchema, metaclass=ResourceSchemaMeta):
    """The fields of the MetroclusterSvmPartnerSvm object"""

    name = fields.Str(data_key="name")
    r""" MetroCluster partner SVM name. """

    uuid = fields.Str(data_key="uuid")
    r""" MetroCluster partner SVM UUID. """

    @property
    def resource(self):
        return MetroclusterSvmPartnerSvm

    gettable_fields = [
        "name",
        "uuid",
    ]
    """name,uuid,"""

    patchable_fields = [
    ]
    """"""

    postable_fields = [
    ]
    """"""


class MetroclusterSvmPartnerSvm(Resource):

    _schema = MetroclusterSvmPartnerSvmSchema
