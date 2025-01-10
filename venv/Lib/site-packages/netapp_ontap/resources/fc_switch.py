r"""
Copyright &copy; 2023 NetApp Inc.
All rights reserved.

This file has been automatically generated based on the ONTAP REST API documentation.

"""

import asyncio
from datetime import datetime
import inspect
from typing import Callable, Iterable, List, Optional, Union

try:
    RECLINE_INSTALLED = False
    import recline
    from recline.arg_types.choices import Choices
    from recline.commands import ReclineCommandError
    from netapp_ontap.resource_table import ResourceTable
    RECLINE_INSTALLED = True
except ImportError:
    pass

from marshmallow import fields, EXCLUDE  # type: ignore

import netapp_ontap
from netapp_ontap.resource import Resource, ResourceSchema, ResourceSchemaMeta, ImpreciseDateTime, Size
from netapp_ontap import NetAppResponse, HostConnection
from netapp_ontap.validations import enum_validation, len_validation, integer_validation
from netapp_ontap.error import NetAppRestError


__all__ = ["FcSwitch", "FcSwitchSchema"]
__pdoc__ = {
    "FcSwitchSchema.resource": False,
    "FcSwitchSchema.opts": False,
    "FcSwitch.fc_switch_show": False,
    "FcSwitch.fc_switch_create": False,
    "FcSwitch.fc_switch_modify": False,
    "FcSwitch.fc_switch_delete": False,
}


class FcSwitchSchema(ResourceSchema, metaclass=ResourceSchemaMeta):
    """The fields of the FcSwitch object"""

    links = fields.Nested("netapp_ontap.models.self_link.SelfLinkSchema", data_key="_links", unknown=EXCLUDE)
    r""" The links field of the fc_switch."""

    cache = fields.Nested("netapp_ontap.models.fabric_cache.FabricCacheSchema", data_key="cache", unknown=EXCLUDE)
    r""" The cache field of the fc_switch."""

    domain_id = Size(
        data_key="domain_id",
        validate=integer_validation(minimum=1, maximum=239),
    )
    r""" The domain identifier (ID) of the Fibre Channel (FC) switch. The domain ID is a unique identifier for the FC switch in the FC fabric.


Example: 1"""

    fabric = fields.Nested("netapp_ontap.resources.fabric.FabricSchema", data_key="fabric", unknown=EXCLUDE)
    r""" The fabric field of the fc_switch."""

    name = fields.Str(
        data_key="name",
    )
    r""" The logical name of the Fibre Channel switch.


Example: switch1"""

    ports = fields.List(fields.Nested("netapp_ontap.models.fc_switch_port.FcSwitchPortSchema", unknown=EXCLUDE), data_key="ports")
    r""" An array of the Fibre Channel (FC) switch's ports and their attached FC devices."""

    release = fields.Str(
        data_key="release",
    )
    r""" The firmware release of the Fibre Channel switch.


Example: 1.0."""

    vendor = fields.Str(
        data_key="vendor",
    )
    r""" The vendor of the Fibre Channel switch.


Example: vendor1"""

    wwn = fields.Str(
        data_key="wwn",
    )
    r""" The world-wide name (WWN) for the Fibre Channel switch.


Example: 10:00:e1:e2:e3:e4:e5:e6"""

    @property
    def resource(self):
        return FcSwitch

    gettable_fields = [
        "links",
        "cache",
        "domain_id",
        "fabric.links",
        "fabric.name",
        "name",
        "ports",
        "release",
        "vendor",
        "wwn",
    ]
    """links,cache,domain_id,fabric.links,fabric.name,name,ports,release,vendor,wwn,"""

    patchable_fields = [
    ]
    """"""

    postable_fields = [
    ]
    """"""

def _get_field_list(field: str) -> Callable[[], List]:
    def getter():
        return [getattr(r, field) for r in FcSwitch.get_collection(fields=field)]
    return getter

async def _wait_for_job(response: NetAppResponse) -> None:
    """Examine the given response. If it is a job, asynchronously wait for it to
    complete. While polling, prints the current status message of the job.
    """

    if not response.is_job:
        return
    from netapp_ontap.resources import Job
    job = Job(**response.http_response.json()["job"])
    while True:
        job.get(fields="state,message")
        if hasattr(job, "message"):
            print("[%s]: %s" % (job.state, job.message))
        if job.state == "failure":
            raise NetAppRestError("FcSwitch modify job failed")
        if job.state == "success":
            break
        await asyncio.sleep(1)

class FcSwitch(Resource):
    r""" A Fibre Channel switch. """

    _schema = FcSwitchSchema
    _path = "/api/network/fc/fabrics/{fabric[name]}/switches"
    _keys = ["fabric.name", "wwn"]

    @classmethod
    def get_collection(
        cls,
        *args,
        connection: HostConnection = None,
        max_records: int = None,
        **kwargs
    ) -> Iterable["Resource"]:
        r"""Retrieves the Fibre Channel switches of a Fibre Channel fabric.
### Expensive properties
There is an added computational cost to retrieving values for these properties. They are not included by default in GET results and must be explicitly requested using the `fields` query parameter. See [`Requesting specific fields`](#Requesting_specific_fields) to learn more.
* `ports`
### Related ONTAP commands
* `network fcp topology show`
### Learn more
* [`DOC /network/fc/fabrics`](#docs-networking-network_fc_fabrics)
"""
        return super()._get_collection(*args, connection=connection, max_records=max_records, **kwargs)

    get_collection.__func__.__doc__ += "\n\n---\n" + inspect.cleandoc(Resource._get_collection.__doc__)

    if RECLINE_INSTALLED:
        @recline.command(name="fc switch show")
        def fc_switch_show(
            fabric_name,
            domain_id: Choices.define(_get_field_list("domain_id"), cache_choices=True, inexact=True)=None,
            name: Choices.define(_get_field_list("name"), cache_choices=True, inexact=True)=None,
            release: Choices.define(_get_field_list("release"), cache_choices=True, inexact=True)=None,
            vendor: Choices.define(_get_field_list("vendor"), cache_choices=True, inexact=True)=None,
            wwn: Choices.define(_get_field_list("wwn"), cache_choices=True, inexact=True)=None,
            fields: List[Choices.define(["domain_id", "name", "release", "vendor", "wwn", "*"])]=None,
        ) -> ResourceTable:
            """Fetch a list of FcSwitch resources

            Args:
                domain_id: The domain identifier (ID) of the Fibre Channel (FC) switch. The domain ID is a unique identifier for the FC switch in the FC fabric. 
                name: The logical name of the Fibre Channel switch. 
                release: The firmware release of the Fibre Channel switch. 
                vendor: The vendor of the Fibre Channel switch. 
                wwn: The world-wide name (WWN) for the Fibre Channel switch. 
            """

            kwargs = {}
            if domain_id is not None:
                kwargs["domain_id"] = domain_id
            if name is not None:
                kwargs["name"] = name
            if release is not None:
                kwargs["release"] = release
            if vendor is not None:
                kwargs["vendor"] = vendor
            if wwn is not None:
                kwargs["wwn"] = wwn
            if fields is not None:
                fields = ",".join(fields)
                kwargs["fields"] = fields

            return FcSwitch.get_collection(
                fabric_name,
                **kwargs
            )

    @classmethod
    def count_collection(
        cls,
        *args,
        connection: HostConnection = None,
        **kwargs
    ) -> int:
        """Returns a count of all FcSwitch resources that match the provided query"""
        return super()._count_collection(*args, connection=connection, **kwargs)

    count_collection.__func__.__doc__ += "\n\n---\n" + inspect.cleandoc(Resource._count_collection.__doc__)




    @classmethod
    def find(cls, *args, connection: HostConnection = None, **kwargs) -> Resource:
        r"""Retrieves the Fibre Channel switches of a Fibre Channel fabric.
### Expensive properties
There is an added computational cost to retrieving values for these properties. They are not included by default in GET results and must be explicitly requested using the `fields` query parameter. See [`Requesting specific fields`](#Requesting_specific_fields) to learn more.
* `ports`
### Related ONTAP commands
* `network fcp topology show`
### Learn more
* [`DOC /network/fc/fabrics`](#docs-networking-network_fc_fabrics)
"""
        return super()._find(*args, connection=connection, **kwargs)

    find.__func__.__doc__ += "\n\n---\n" + inspect.cleandoc(Resource._find.__doc__)

    def get(self, **kwargs) -> NetAppResponse:
        r"""Retrieves a Fibre Channel switch.
### Expensive properties
There is an added computational cost to retrieving values for these properties. They are not included by default in GET results and must be explicitly requested using the `fields` query parameter. See [`Requesting specific fields`](#Requesting_specific_fields) to learn more.
* `ports`
### Related ONTAP commands
* `network fcp topology show`
### Learn more
* [`DOC /network/fc/fabrics`](#docs-networking-network_fc_fabrics)
"""
        return super()._get(**kwargs)

    get.__doc__ += "\n\n---\n" + inspect.cleandoc(Resource._get.__doc__)





