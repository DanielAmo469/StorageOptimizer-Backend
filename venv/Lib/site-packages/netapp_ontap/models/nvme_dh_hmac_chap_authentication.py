r"""
Copyright &copy; 2023 NetApp Inc.
All rights reserved.

This file has been automatically generated based on the ONTAP REST API documentation.

"""

from marshmallow import EXCLUDE, fields  # type: ignore
from netapp_ontap.resource import Resource, ResourceSchema, ResourceSchemaMeta, ImpreciseDateTime, Size


__all__ = ["NvmeDhHmacChapAuthentication", "NvmeDhHmacChapAuthenticationSchema"]
__pdoc__ = {
    "NvmeDhHmacChapAuthenticationSchema.resource": False,
    "NvmeDhHmacChapAuthenticationSchema.opts": False,
    "NvmeDhHmacChapAuthentication": False,
}


class NvmeDhHmacChapAuthenticationSchema(ResourceSchema, metaclass=ResourceSchemaMeta):
    """The fields of the NvmeDhHmacChapAuthentication object"""

    controller_secret_key = fields.Str(data_key="controller_secret_key")
    r""" The controller secret for NVMe in-band authentication. The value of this property is used by the NVMe host to authenticate the NVMe controller while establishing a connection. If unset, the controller is not authenticated. When supplied, the property `host_secret_key` must also be supplied. Optional in POST.<br/>
This property is write-only. The `mode` property can be used to identify if a controller secret has been set for the host, but the controller secret value cannot be read. To change the value, the host must be deleted from the subsystem and re-added.


Example: DHHC-1:00:ia6zGodOr4SEG0Zzaw398rpY0wqipUWj4jWjUh4HWUz6aQ2n: """

    group_size = fields.Str(data_key="group_size")
    r""" The Diffie-Hellman group size for NVMe in-band authentication. When property `host_secret_key` is provided, this property defaults to `2048_bit`. When supplied, the property `host_secret_key` must also be supplied. Optional in POST.


Valid choices:

* none
* 2048_bit
* 3072_bit
* 4096_bit
* 6144_bit
* 8192_bit """

    hash_function = fields.Str(data_key="hash_function")
    r""" The hash function for NVMe in-band authentication. When property `host_secret_key` is provided, this property defaults to `sha_256`. When supplied, the property `host_secret_key` must also be supplied. Optional in POST.


Valid choices:

* sha_256
* sha_512 """

    host_secret_key = fields.Str(data_key="host_secret_key")
    r""" The host secret for NVMe in-band authentication. The value of this property is used by the NVMe controller to authenticate the NVMe host while establishing a connection. If unset, no authentication is performed by the host or controller. This property must be supplied if any other NVMe in-band authentication properties are supplied. Optional in POST.<br/>
This property is write-only. The `mode` property can be used to identify if a host secret has been set for the host, but the host secret value cannot be read. To change the value, the host must be deleted from the subsystem and re-added.


Example: DHHC-1:00:ia6zGodOr4SEG0Zzaw398rpY0wqipUWj4jWjUh4HWUz6aQ2n: """

    mode = fields.Str(data_key="mode")
    r""" The expected NVMe in-band authentication mode for the host. This property is an indication of which secrets are configured for the host. When set to:
- none: The host has neither the host nor controller secret configured, and no authentication is performed.
- unidirectional: The host has a host secret configured. The controller will authenticate the host.
- bidirectional: The host has both a host and controller secret configured. The controller will authenticate the host and the host will authenticate the controller.


Valid choices:

* none
* unidirectional
* bidirectional """

    @property
    def resource(self):
        return NvmeDhHmacChapAuthentication

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
        "controller_secret_key",
        "group_size",
        "hash_function",
        "host_secret_key",
    ]
    """controller_secret_key,group_size,hash_function,host_secret_key,"""


class NvmeDhHmacChapAuthentication(Resource):

    _schema = NvmeDhHmacChapAuthenticationSchema
