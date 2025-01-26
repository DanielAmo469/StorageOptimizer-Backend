r"""
Copyright &copy; 2023 NetApp Inc.
All rights reserved.

This file has been automatically generated based on the ONTAP REST API documentation.

"""

from marshmallow import EXCLUDE, fields  # type: ignore
from netapp_ontap.resource import Resource, ResourceSchema, ResourceSchemaMeta, ImpreciseDateTime, Size


__all__ = ["NasApplicationComponentsFlexcacheOrigin", "NasApplicationComponentsFlexcacheOriginSchema"]
__pdoc__ = {
    "NasApplicationComponentsFlexcacheOriginSchema.resource": False,
    "NasApplicationComponentsFlexcacheOriginSchema.opts": False,
    "NasApplicationComponentsFlexcacheOrigin": False,
}


class NasApplicationComponentsFlexcacheOriginSchema(ResourceSchema, metaclass=ResourceSchemaMeta):
    """The fields of the NasApplicationComponentsFlexcacheOrigin object"""

    component = fields.Nested("netapp_ontap.models.nas_application_components_flexcache_origin_component.NasApplicationComponentsFlexcacheOriginComponentSchema", unknown=EXCLUDE, data_key="component")
    r""" The component field of the nas_application_components_flexcache_origin. """

    svm = fields.Nested("netapp_ontap.models.nas_application_components_flexcache_origin_svm.NasApplicationComponentsFlexcacheOriginSvmSchema", unknown=EXCLUDE, data_key="svm")
    r""" The svm field of the nas_application_components_flexcache_origin. """

    @property
    def resource(self):
        return NasApplicationComponentsFlexcacheOrigin

    gettable_fields = [
        "component",
        "svm",
    ]
    """component,svm,"""

    patchable_fields = [
    ]
    """"""

    postable_fields = [
        "component",
        "svm",
    ]
    """component,svm,"""


class NasApplicationComponentsFlexcacheOrigin(Resource):

    _schema = NasApplicationComponentsFlexcacheOriginSchema
