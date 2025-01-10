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


__all__ = ["FcZone", "FcZoneSchema"]
__pdoc__ = {
    "FcZoneSchema.resource": False,
    "FcZoneSchema.opts": False,
    "FcZone.fc_zone_show": False,
    "FcZone.fc_zone_create": False,
    "FcZone.fc_zone_modify": False,
    "FcZone.fc_zone_delete": False,
}


class FcZoneSchema(ResourceSchema, metaclass=ResourceSchemaMeta):
    """The fields of the FcZone object"""

    links = fields.Nested("netapp_ontap.models.self_link.SelfLinkSchema", data_key="_links", unknown=EXCLUDE)
    r""" The links field of the fc_zone."""

    cache = fields.Nested("netapp_ontap.models.fabric_cache.FabricCacheSchema", data_key="cache", unknown=EXCLUDE)
    r""" The cache field of the fc_zone."""

    fabric = fields.Nested("netapp_ontap.resources.fabric.FabricSchema", data_key="fabric", unknown=EXCLUDE)
    r""" The fabric field of the fc_zone."""

    members = fields.List(fields.Nested("netapp_ontap.models.fc_zone_member.FcZoneMemberSchema", unknown=EXCLUDE), data_key="members")
    r""" An array of Fibre Channel zone members."""

    name = fields.Str(
        data_key="name",
    )
    r""" The name of the Fibre Channel zone.


Example: zone1"""

    @property
    def resource(self):
        return FcZone

    gettable_fields = [
        "links",
        "cache",
        "fabric.links",
        "fabric.name",
        "members",
        "name",
    ]
    """links,cache,fabric.links,fabric.name,members,name,"""

    patchable_fields = [
    ]
    """"""

    postable_fields = [
    ]
    """"""

def _get_field_list(field: str) -> Callable[[], List]:
    def getter():
        return [getattr(r, field) for r in FcZone.get_collection(fields=field)]
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
            raise NetAppRestError("FcZone modify job failed")
        if job.state == "success":
            break
        await asyncio.sleep(1)

class FcZone(Resource):
    r""" A Fibre Channel zone. """

    _schema = FcZoneSchema
    _path = "/api/network/fc/fabrics/{fabric[name]}/zones"
    _keys = ["fabric.name", "name"]

    @classmethod
    def get_collection(
        cls,
        *args,
        connection: HostConnection = None,
        max_records: int = None,
        **kwargs
    ) -> Iterable["Resource"]:
        r"""Retrieves the zones of the active zoneset of a Fibre Channel fabric.
### Expensive properties
There is an added computational cost to retrieving values for these properties. They are not included by default in GET results and must be explicitly requested using the `fields` query parameter. See [`Requesting specific fields`](#Requesting_specific_fields) to learn more.
* `members`
### Related ONTAP commands
* `network fcp zone show`
### Learn more
* [`DOC /network/fc/fabrics`](#docs-networking-network_fc_fabrics)
"""
        return super()._get_collection(*args, connection=connection, max_records=max_records, **kwargs)

    get_collection.__func__.__doc__ += "\n\n---\n" + inspect.cleandoc(Resource._get_collection.__doc__)

    if RECLINE_INSTALLED:
        @recline.command(name="fc zone show")
        def fc_zone_show(
            fabric_name,
            name: Choices.define(_get_field_list("name"), cache_choices=True, inexact=True)=None,
            fields: List[Choices.define(["name", "*"])]=None,
        ) -> ResourceTable:
            """Fetch a list of FcZone resources

            Args:
                name: The name of the Fibre Channel zone. 
            """

            kwargs = {}
            if name is not None:
                kwargs["name"] = name
            if fields is not None:
                fields = ",".join(fields)
                kwargs["fields"] = fields

            return FcZone.get_collection(
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
        """Returns a count of all FcZone resources that match the provided query"""
        return super()._count_collection(*args, connection=connection, **kwargs)

    count_collection.__func__.__doc__ += "\n\n---\n" + inspect.cleandoc(Resource._count_collection.__doc__)




    @classmethod
    def find(cls, *args, connection: HostConnection = None, **kwargs) -> Resource:
        r"""Retrieves the zones of the active zoneset of a Fibre Channel fabric.
### Expensive properties
There is an added computational cost to retrieving values for these properties. They are not included by default in GET results and must be explicitly requested using the `fields` query parameter. See [`Requesting specific fields`](#Requesting_specific_fields) to learn more.
* `members`
### Related ONTAP commands
* `network fcp zone show`
### Learn more
* [`DOC /network/fc/fabrics`](#docs-networking-network_fc_fabrics)
"""
        return super()._find(*args, connection=connection, **kwargs)

    find.__func__.__doc__ += "\n\n---\n" + inspect.cleandoc(Resource._find.__doc__)

    def get(self, **kwargs) -> NetAppResponse:
        r"""Retrieves a zone of the active zoneset of a Fibre Channel fabric.
### Expensive properties
There is an added computational cost to retrieving values for these properties. They are not included by default in GET results and must be explicitly requested using the `fields` query parameter. See [`Requesting specific fields`](#Requesting_specific_fields) to learn more.
* `members`
### Related ONTAP commands
* `network fcp zone show`
### Learn more
* [`DOC /network/fc/fabrics`](#docs-networking-network_fc_fabrics)
"""
        return super()._get(**kwargs)

    get.__doc__ += "\n\n---\n" + inspect.cleandoc(Resource._get.__doc__)





