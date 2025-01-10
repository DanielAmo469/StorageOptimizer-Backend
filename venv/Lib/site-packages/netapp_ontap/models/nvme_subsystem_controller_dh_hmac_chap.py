r"""
Copyright &copy; 2023 NetApp Inc.
All rights reserved.

This file has been automatically generated based on the ONTAP REST API documentation.

"""

from marshmallow import EXCLUDE, fields  # type: ignore
from netapp_ontap.resource import Resource, ResourceSchema, ResourceSchemaMeta, ImpreciseDateTime, Size


__all__ = ["NvmeSubsystemControllerDhHmacChap", "NvmeSubsystemControllerDhHmacChapSchema"]
__pdoc__ = {
    "NvmeSubsystemControllerDhHmacChapSchema.resource": False,
    "NvmeSubsystemControllerDhHmacChapSchema.opts": False,
    "NvmeSubsystemControllerDhHmacChap": False,
}


class NvmeSubsystemControllerDhHmacChapSchema(ResourceSchema, metaclass=ResourceSchemaMeta):
    """The fields of the NvmeSubsystemControllerDhHmacChap object"""

    group_size = fields.Str(data_key="group_size")
    r""" The Diffie-Hellman group size used for NVMe in-band authentication.


Valid choices:

* none
* 2048_bit
* 3072_bit
* 4096_bit
* 6144_bit
* 8192_bit """

    hash_function = fields.Str(data_key="hash_function")
    r""" The hash function used for NVMe in-band authentication.


Valid choices:

* sha_256
* sha_512 """

    mode = fields.Str(data_key="mode")
    r""" The NVMe in-band authentication mode used for the host connection. When set to:
- none: Neither the host nor controller was authenticated.
- unidirectional: The controller authenticated the host.
- bidirectional: The controller authenticated the host and the host authenticated the controller.


Valid choices:

* none
* unidirectional
* bidirectional """

    @property
    def resource(self):
        return NvmeSubsystemControllerDhHmacChap

    gettable_fields = [
        "group_size",
        "hash_function",
        "mode",
    ]
    """group_size,hash_function,mode,"""

    patchable_fields = [
    ]
    """"""

    postable_fields = [
    ]
    """"""


class NvmeSubsystemControllerDhHmacChap(Resource):

    _schema = NvmeSubsystemControllerDhHmacChapSchema
