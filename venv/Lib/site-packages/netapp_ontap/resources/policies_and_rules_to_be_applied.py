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


__all__ = ["PoliciesAndRulesToBeApplied", "PoliciesAndRulesToBeAppliedSchema"]
__pdoc__ = {
    "PoliciesAndRulesToBeAppliedSchema.resource": False,
    "PoliciesAndRulesToBeAppliedSchema.opts": False,
    "PoliciesAndRulesToBeApplied.policies_and_rules_to_be_applied_show": False,
    "PoliciesAndRulesToBeApplied.policies_and_rules_to_be_applied_create": False,
    "PoliciesAndRulesToBeApplied.policies_and_rules_to_be_applied_modify": False,
    "PoliciesAndRulesToBeApplied.policies_and_rules_to_be_applied_delete": False,
}


class PoliciesAndRulesToBeAppliedSchema(ResourceSchema, metaclass=ResourceSchemaMeta):
    """The fields of the PoliciesAndRulesToBeApplied object"""

    svm = fields.Nested("netapp_ontap.resources.svm.SvmSchema", data_key="svm", unknown=EXCLUDE)
    r""" The svm field of the policies_and_rules_to_be_applied."""

    to_be_applied = fields.Nested("netapp_ontap.models.policies_and_rules_to_be_applied_to_be_applied.PoliciesAndRulesToBeAppliedToBeAppliedSchema", data_key="to_be_applied", unknown=EXCLUDE)
    r""" The to_be_applied field of the policies_and_rules_to_be_applied."""

    @property
    def resource(self):
        return PoliciesAndRulesToBeApplied

    gettable_fields = [
        "svm.links",
        "svm.name",
        "svm.uuid",
        "to_be_applied",
    ]
    """svm.links,svm.name,svm.uuid,to_be_applied,"""

    patchable_fields = [
    ]
    """"""

    postable_fields = [
    ]
    """"""

def _get_field_list(field: str) -> Callable[[], List]:
    def getter():
        return [getattr(r, field) for r in PoliciesAndRulesToBeApplied.get_collection(fields=field)]
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
            raise NetAppRestError("PoliciesAndRulesToBeApplied modify job failed")
        if job.state == "success":
            break
        await asyncio.sleep(1)

class PoliciesAndRulesToBeApplied(Resource):
    """Allows interaction with PoliciesAndRulesToBeApplied objects on the host"""

    _schema = PoliciesAndRulesToBeAppliedSchema
    _path = "/api/protocols/cifs/group-policies"
    _keys = ["svm.uuid"]

    @classmethod
    def get_collection(
        cls,
        *args,
        connection: HostConnection = None,
        max_records: int = None,
        **kwargs
    ) -> Iterable["Resource"]:
        r"""Retrieves group policy objects that are yet to be applied for all SVMs.
### Related ONTAP commands
* `vserver cifs group-policy show-defined`
"""
        return super()._get_collection(*args, connection=connection, max_records=max_records, **kwargs)

    get_collection.__func__.__doc__ += "\n\n---\n" + inspect.cleandoc(Resource._get_collection.__doc__)

    if RECLINE_INSTALLED:
        @recline.command(name="policies and rules to be applied show")
        def policies_and_rules_to_be_applied_show(
            fields: List[Choices.define(["*"])]=None,
        ) -> ResourceTable:
            """Fetch a list of PoliciesAndRulesToBeApplied resources

            Args:
            """

            kwargs = {}
            if fields is not None:
                fields = ",".join(fields)
                kwargs["fields"] = fields

            return PoliciesAndRulesToBeApplied.get_collection(
                **kwargs
            )

    @classmethod
    def count_collection(
        cls,
        *args,
        connection: HostConnection = None,
        **kwargs
    ) -> int:
        """Returns a count of all PoliciesAndRulesToBeApplied resources that match the provided query"""
        return super()._count_collection(*args, connection=connection, **kwargs)

    count_collection.__func__.__doc__ += "\n\n---\n" + inspect.cleandoc(Resource._count_collection.__doc__)

    @classmethod
    def patch_collection(
        cls,
        body: dict,
        *args,
        records: Iterable["PoliciesAndRulesToBeApplied"] = None,
        poll: bool = True,
        poll_interval: Optional[int] = None,
        poll_timeout: Optional[int] = None,
        connection: HostConnection = None,
        **kwargs
    ) -> NetAppResponse:
        r"""Will create a background task to update the GPO settings for specified SVM.
### Related ONTAP commands
* `vserver cifs group-policy update`
"""
        return super()._patch_collection(
            body, *args, records=records, poll=poll, poll_interval=poll_interval,
            poll_timeout=poll_timeout, connection=connection, **kwargs
        )

    patch_collection.__func__.__doc__ += "\n\n---\n" + inspect.cleandoc(Resource._patch_collection.__doc__)



    @classmethod
    def find(cls, *args, connection: HostConnection = None, **kwargs) -> Resource:
        r"""Retrieves group policy objects that are yet to be applied for all SVMs.
### Related ONTAP commands
* `vserver cifs group-policy show-defined`
"""
        return super()._find(*args, connection=connection, **kwargs)

    find.__func__.__doc__ += "\n\n---\n" + inspect.cleandoc(Resource._find.__doc__)

    def get(self, **kwargs) -> NetAppResponse:
        r"""Retrieves group policy objects that are yet to be applied for specified SVM.
### Related ONTAP commands
* `vserver cifs group-policy show-defined`
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
        r"""Will create a background task to update the GPO settings for specified SVM.
### Related ONTAP commands
* `vserver cifs group-policy update`
"""
        return super()._patch(
            hydrate=hydrate, poll=poll, poll_interval=poll_interval,
            poll_timeout=poll_timeout, **kwargs
        )

    patch.__doc__ += "\n\n---\n" + inspect.cleandoc(Resource._patch.__doc__)

    if RECLINE_INSTALLED:
        @recline.command(name="policies and rules to be applied modify")
        async def policies_and_rules_to_be_applied_modify(
        ) -> ResourceTable:
            """Modify an instance of a PoliciesAndRulesToBeApplied resource

            Args:
            """

            kwargs = {}
            changes = {}


            if hasattr(PoliciesAndRulesToBeApplied, "find"):
                resource = PoliciesAndRulesToBeApplied.find(
                    **kwargs
                )
            else:
                resource = PoliciesAndRulesToBeApplied()
            try:
                for key, value in changes.items():
                    setattr(resource, key, value)
                response = resource.patch(poll=False)
                await _wait_for_job(response)
                resource.get(fields=",".join(changes.keys()))
                return [resource]
            except NetAppRestError as err:
                raise ReclineCommandError("Unable to modify PoliciesAndRulesToBeApplied: %s" % err)



