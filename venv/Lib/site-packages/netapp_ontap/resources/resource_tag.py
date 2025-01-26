r"""
Copyright &copy; 2023 NetApp Inc.
All rights reserved.

This file has been automatically generated based on the ONTAP REST API documentation.

# Overview
You can use this endpoint to list all of the tags that have been used on resources in the API. See the section labelled [Tagging Resources for Tracking Purposes](#Tagging_Resources_for_Tracking_Purposes) to find out more information about how to tag a resource.
## Examples
The following examples show some ways that this endpoint can be used.
### List all of the used tags
```python
from netapp_ontap import HostConnection
from netapp_ontap.resources import ResourceTag

with HostConnection("<mgmt-ip>", username="admin", password="password", verify=False):
    print(list(ResourceTag.get_collection()))

```
<div class="try_it_out">
<input id="example0_try_it_out" type="checkbox", class="try_it_out_check">
<label for="example0_try_it_out" class="try_it_out_button">Try it out</label>
<div id="example0_result" class="try_it_out_content">
```
[
    ResourceTag({"value": "team:accounting", "num_resources": 2}),
    ResourceTag({"value": "environment:test", "num_resources": 5}),
]

```
</div>
</div>

---
### Find tags that are being used by at least 3 resources
```python
from netapp_ontap import HostConnection
from netapp_ontap.resources import ResourceTag

with HostConnection("<mgmt-ip>", username="admin", password="password", verify=False):
    print(list(ResourceTag.get_collection(num_resources=">=3")))

```
<div class="try_it_out">
<input id="example1_try_it_out" type="checkbox", class="try_it_out_check">
<label for="example1_try_it_out" class="try_it_out_button">Try it out</label>
<div id="example1_result" class="try_it_out_content">
```
[ResourceTag({"value": "environment:test", "num_resources": 5})]

```
</div>
</div>
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


__all__ = ["ResourceTag", "ResourceTagSchema"]
__pdoc__ = {
    "ResourceTagSchema.resource": False,
    "ResourceTagSchema.opts": False,
    "ResourceTag.resource_tag_show": False,
    "ResourceTag.resource_tag_create": False,
    "ResourceTag.resource_tag_modify": False,
    "ResourceTag.resource_tag_delete": False,
}


class ResourceTagSchema(ResourceSchema, metaclass=ResourceSchemaMeta):
    """The fields of the ResourceTag object"""

    num_resources = Size(
        data_key="num_resources",
        validate=integer_validation(minimum=1),
    )
    r""" The number of resources that are currently using this tag."""

    value = fields.Str(
        data_key="value",
    )
    r""" A key:value formatted string representing the tag's name.

Example: team:accounting"""

    @property
    def resource(self):
        return ResourceTag

    gettable_fields = [
        "num_resources",
        "value",
    ]
    """num_resources,value,"""

    patchable_fields = [
        "num_resources",
        "value",
    ]
    """num_resources,value,"""

    postable_fields = [
        "num_resources",
        "value",
    ]
    """num_resources,value,"""

def _get_field_list(field: str) -> Callable[[], List]:
    def getter():
        return [getattr(r, field) for r in ResourceTag.get_collection(fields=field)]
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
            raise NetAppRestError("ResourceTag modify job failed")
        if job.state == "success":
            break
        await asyncio.sleep(1)

class ResourceTag(Resource):
    r""" A resource tag is a way to group resources in the API together for identification or tracking purposes. """

    _schema = ResourceTagSchema
    _path = "/api/resource-tags"
    _keys = ["value"]

    @classmethod
    def get_collection(
        cls,
        *args,
        connection: HostConnection = None,
        max_records: int = None,
        **kwargs
    ) -> Iterable["Resource"]:
        r"""Retrieves the tags currently being used for resources in the API.
### Learn more
* [`DOC /resource-tags`](#docs-cluster-resource-tags)"""
        return super()._get_collection(*args, connection=connection, max_records=max_records, **kwargs)

    get_collection.__func__.__doc__ += "\n\n---\n" + inspect.cleandoc(Resource._get_collection.__doc__)

    if RECLINE_INSTALLED:
        @recline.command(name="resource tag show")
        def resource_tag_show(
            fields: List[Choices.define(["num_resources", "value", "*"])]=None,
        ) -> ResourceTable:
            """Fetch a list of ResourceTag resources

            Args:
                num_resources: The number of resources that are currently using this tag.
                value: A key:value formatted string representing the tag's name.
            """

            kwargs = {}
            if num_resources is not None:
                kwargs["num_resources"] = num_resources
            if value is not None:
                kwargs["value"] = value
            if fields is not None:
                fields = ",".join(fields)
                kwargs["fields"] = fields

            return ResourceTag.get_collection(
                **kwargs
            )

    @classmethod
    def count_collection(
        cls,
        *args,
        connection: HostConnection = None,
        **kwargs
    ) -> int:
        """Returns a count of all ResourceTag resources that match the provided query"""
        return super()._count_collection(*args, connection=connection, **kwargs)

    count_collection.__func__.__doc__ += "\n\n---\n" + inspect.cleandoc(Resource._count_collection.__doc__)




    @classmethod
    def find(cls, *args, connection: HostConnection = None, **kwargs) -> Resource:
        r"""Retrieves the tags currently being used for resources in the API.
### Learn more
* [`DOC /resource-tags`](#docs-cluster-resource-tags)"""
        return super()._find(*args, connection=connection, **kwargs)

    find.__func__.__doc__ += "\n\n---\n" + inspect.cleandoc(Resource._find.__doc__)

    def get(self, **kwargs) -> NetAppResponse:
        r"""Retrieves a specific resource tag.
### Learn more
* [`DOC /resource-tags`](#docs-cluster-resource-tags)"""
        return super()._get(**kwargs)

    get.__doc__ += "\n\n---\n" + inspect.cleandoc(Resource._get.__doc__)





