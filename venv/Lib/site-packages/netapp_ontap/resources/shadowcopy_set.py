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


__all__ = ["ShadowcopySet", "ShadowcopySetSchema"]
__pdoc__ = {
    "ShadowcopySetSchema.resource": False,
    "ShadowcopySetSchema.opts": False,
    "ShadowcopySet.shadowcopy_set_show": False,
    "ShadowcopySet.shadowcopy_set_create": False,
    "ShadowcopySet.shadowcopy_set_modify": False,
    "ShadowcopySet.shadowcopy_set_delete": False,
}


class ShadowcopySetSchema(ResourceSchema, metaclass=ResourceSchemaMeta):
    """The fields of the ShadowcopySet object"""

    keep_snapshots = fields.Boolean(
        data_key="keep_snapshots",
    )
    r""" Request the storage system to keep the snapshot copies taken as a part of the shadow copy set creation.

Example: false"""

    svm = fields.Nested("netapp_ontap.resources.svm.SvmSchema", data_key="svm", unknown=EXCLUDE)
    r""" The svm field of the shadowcopy_set."""

    uuid = fields.Str(
        data_key="uuid",
    )
    r""" The universally-unique identifier of the storage's shadow copy set.

Example: f8328660-00e6-11e6-80d9-005056bd65a9"""

    @property
    def resource(self):
        return ShadowcopySet

    gettable_fields = [
        "keep_snapshots",
        "svm.links",
        "svm.name",
        "svm.uuid",
        "uuid",
    ]
    """keep_snapshots,svm.links,svm.name,svm.uuid,uuid,"""

    patchable_fields = [
        "keep_snapshots",
        "svm.name",
        "svm.uuid",
    ]
    """keep_snapshots,svm.name,svm.uuid,"""

    postable_fields = [
        "keep_snapshots",
        "svm.name",
        "svm.uuid",
    ]
    """keep_snapshots,svm.name,svm.uuid,"""

def _get_field_list(field: str) -> Callable[[], List]:
    def getter():
        return [getattr(r, field) for r in ShadowcopySet.get_collection(fields=field)]
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
            raise NetAppRestError("ShadowcopySet modify job failed")
        if job.state == "success":
            break
        await asyncio.sleep(1)

class ShadowcopySet(Resource):
    """Allows interaction with ShadowcopySet objects on the host"""

    _schema = ShadowcopySetSchema
    _path = "/api/protocols/cifs/shadowcopy-sets"
    _keys = ["uuid"]

    @classmethod
    def get_collection(
        cls,
        *args,
        connection: HostConnection = None,
        max_records: int = None,
        **kwargs
    ) -> Iterable["Resource"]:
        r"""Retrieves Shadowcopy Sets.
### Related ONTAP commands
* `vserver cifs shadowcopy show-sets`
### Learn more
* [`DOC /protocols/cifs/shadow-copies`](#docs-NAS-protocols_cifs_shadow-copies)
"""
        return super()._get_collection(*args, connection=connection, max_records=max_records, **kwargs)

    get_collection.__func__.__doc__ += "\n\n---\n" + inspect.cleandoc(Resource._get_collection.__doc__)

    if RECLINE_INSTALLED:
        @recline.command(name="shadowcopy set show")
        def shadowcopy_set_show(
            fields: List[Choices.define(["keep_snapshots", "uuid", "*"])]=None,
        ) -> ResourceTable:
            """Fetch a list of ShadowcopySet resources

            Args:
                keep_snapshots: Request the storage system to keep the snapshot copies taken as a part of the shadow copy set creation.
                uuid: The universally-unique identifier of the storage's shadow copy set.
            """

            kwargs = {}
            if keep_snapshots is not None:
                kwargs["keep_snapshots"] = keep_snapshots
            if uuid is not None:
                kwargs["uuid"] = uuid
            if fields is not None:
                fields = ",".join(fields)
                kwargs["fields"] = fields

            return ShadowcopySet.get_collection(
                **kwargs
            )

    @classmethod
    def count_collection(
        cls,
        *args,
        connection: HostConnection = None,
        **kwargs
    ) -> int:
        """Returns a count of all ShadowcopySet resources that match the provided query"""
        return super()._count_collection(*args, connection=connection, **kwargs)

    count_collection.__func__.__doc__ += "\n\n---\n" + inspect.cleandoc(Resource._count_collection.__doc__)

    @classmethod
    def patch_collection(
        cls,
        body: dict,
        *args,
        records: Iterable["ShadowcopySet"] = None,
        poll: bool = True,
        poll_interval: Optional[int] = None,
        poll_timeout: Optional[int] = None,
        connection: HostConnection = None,
        **kwargs
    ) -> NetAppResponse:
        r"""Updates a Shadowcopy set
### Learn more
* [`DOC /protocols/cifs/shadowcopy`](#docs-NAS-protocols_cifs_shadowcopy)
"""
        return super()._patch_collection(
            body, *args, records=records, poll=poll, poll_interval=poll_interval,
            poll_timeout=poll_timeout, connection=connection, **kwargs
        )

    patch_collection.__func__.__doc__ += "\n\n---\n" + inspect.cleandoc(Resource._patch_collection.__doc__)



    @classmethod
    def find(cls, *args, connection: HostConnection = None, **kwargs) -> Resource:
        r"""Retrieves Shadowcopy Sets.
### Related ONTAP commands
* `vserver cifs shadowcopy show-sets`
### Learn more
* [`DOC /protocols/cifs/shadow-copies`](#docs-NAS-protocols_cifs_shadow-copies)
"""
        return super()._find(*args, connection=connection, **kwargs)

    find.__func__.__doc__ += "\n\n---\n" + inspect.cleandoc(Resource._find.__doc__)

    def get(self, **kwargs) -> NetAppResponse:
        r"""Retrieves a Shadowcopy set
### Related ONTAP commands
* `vserver cifs shadowcopy show-sets`
### Learn more
* [`DOC /protocols/cifs/shadow-copies`](#docs-NAS-protocols_cifs_shadow-copies)
"""
        return super()._get(**kwargs)

    get.__doc__ += "\n\n---\n" + inspect.cleandoc(Resource._get.__doc__)


    def patch(
        self,
        hydrate: bool = False,
        poll: bool = True,
        poll_interval: Optional[int] = None,
        poll_timeout: Optional[int] = None,
        **kwargs
    ) -> NetAppResponse:
        r"""Updates a Shadowcopy set
### Learn more
* [`DOC /protocols/cifs/shadowcopy`](#docs-NAS-protocols_cifs_shadowcopy)
"""
        return super()._patch(
            hydrate=hydrate, poll=poll, poll_interval=poll_interval,
            poll_timeout=poll_timeout, **kwargs
        )

    patch.__doc__ += "\n\n---\n" + inspect.cleandoc(Resource._patch.__doc__)

    if RECLINE_INSTALLED:
        @recline.command(name="shadowcopy set modify")
        async def shadowcopy_set_modify(
        ) -> ResourceTable:
            """Modify an instance of a ShadowcopySet resource

            Args:
                keep_snapshots: Request the storage system to keep the snapshot copies taken as a part of the shadow copy set creation.
                query_keep_snapshots: Request the storage system to keep the snapshot copies taken as a part of the shadow copy set creation.
                uuid: The universally-unique identifier of the storage's shadow copy set.
                query_uuid: The universally-unique identifier of the storage's shadow copy set.
            """

            kwargs = {}
            changes = {}
            if query_keep_snapshots is not None:
                kwargs["keep_snapshots"] = query_keep_snapshots
            if query_uuid is not None:
                kwargs["uuid"] = query_uuid

            if keep_snapshots is not None:
                changes["keep_snapshots"] = keep_snapshots
            if uuid is not None:
                changes["uuid"] = uuid

            if hasattr(ShadowcopySet, "find"):
                resource = ShadowcopySet.find(
                    **kwargs
                )
            else:
                resource = ShadowcopySet()
            try:
                for key, value in changes.items():
                    setattr(resource, key, value)
                response = resource.patch(poll=False)
                await _wait_for_job(response)
                resource.get(fields=",".join(changes.keys()))
                return [resource]
            except NetAppRestError as err:
                raise ReclineCommandError("Unable to modify ShadowcopySet: %s" % err)



