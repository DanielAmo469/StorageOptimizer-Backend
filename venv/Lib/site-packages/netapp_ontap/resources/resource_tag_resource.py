r"""
Copyright &copy; 2023 NetApp Inc.
All rights reserved.

This file has been automatically generated based on the ONTAP REST API documentation.

# Overview
You can use this endpoint to list all of the resources in the API that have been tagged with a particular tag value. See the section labelled [Tagging Resources for Tracking Purposes](#Tagging_Resources_for_Tracking_Purposes) to find out more information about how to tag a resource.
## Examples
The following examples show some ways that this endpoint can be used.
### List all resources that are tagged for the test environment.
```python
from netapp_ontap import HostConnection
from netapp_ontap.resources import ResourceTagResource

with HostConnection("<mgmt-ip>", username="admin", password="password", verify=False):
    print(list(ResourceTagResource.get_collection("environment:test")))

```
<div class="try_it_out">
<input id="example0_try_it_out" type="checkbox", class="try_it_out_check">
<label for="example0_try_it_out" class="try_it_out_button">Try it out</label>
<div id="example0_result" class="try_it_out_content">
```
[
    ResourceTagResource(
        {"href": "/api/storage/volumes/558949d1-d4cf-445e-ada5-e340dee6a581"}
    ),
    ResourceTagResource({"href": "/api/svm/svms/7f97a0b1-fe4f-11e8-b9c5-005056a76061"}),
    ResourceTagResource({"href": "/api/cluster"}),
]

```
</div>
</div>

---
### List all volumes that have been tagged for the accounting team
```python
from netapp_ontap import HostConnection
from netapp_ontap.resources import ResourceTagResource

with HostConnection("<mgmt-ip>", username="admin", password="password", verify=False):
    print(
        list(
            ResourceTagResource.get_collection(
                "team:accounting", label="storage_volumes"
            )
        )
    )

```
<div class="try_it_out">
<input id="example1_try_it_out" type="checkbox", class="try_it_out_check">
<label for="example1_try_it_out" class="try_it_out_button">Try it out</label>
<div id="example1_result" class="try_it_out_content">
```
[
    ResourceTagResource(
        {
            "href": "/api/storage/volumes/558949d1-d4cf-445e-ada5-e340dee6a581",
            "label": "storage_volumes",
        }
    ),
    ResourceTagResource(
        {
            "href": "/api/storage/volumes/64750961-fda7-4327-9f16-00034c3f5ad2",
            "label": "storage_volumes",
        }
    ),
    ResourceTagResource(
        {
            "href": "/api/storage/volumes/bee17b91-f90a-4854-b146-8b102a0a9882",
            "label": "storage_volumes",
        }
    ),
]

```
</div>
</div>

---
### Create a new tag on a volume
```python
from netapp_ontap import HostConnection
from netapp_ontap.resources import ResourceTagResource

with HostConnection("<mgmt-ip>", username="admin", password="password", verify=False):
    resource = ResourceTagResource("team:accounting")
    resource.href = "/api/storage/volumes/f288168e-bd3e-11ed-9516-005056acd4e8"
    resource.post(hydrate=True)
    print(resource)

```
<div class="try_it_out">
<input id="example2_try_it_out" type="checkbox", class="try_it_out_check">
<label for="example2_try_it_out" class="try_it_out_button">Try it out</label>
<div id="example2_result" class="try_it_out_content">
```
ResourceTagResource(
    {"href": "/api/storage/volumes/f288168e-bd3e-11ed-9516-005056acd4e8"}
)

```
</div>
</div>

---
### Delete a new tag on a volume
```python
from netapp_ontap import HostConnection
from netapp_ontap.resources import ResourceTagResource

with HostConnection("<mgmt-ip>", username="admin", password="password", verify=False):
    resource = ResourceTagResource(
        "team:accounting",
        href="/api/storage/volumes/f288168e-bd3e-11ed-9516-005056acd4e8",
    )
    resource.delete()

```
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


__all__ = ["ResourceTagResource", "ResourceTagResourceSchema"]
__pdoc__ = {
    "ResourceTagResourceSchema.resource": False,
    "ResourceTagResourceSchema.opts": False,
    "ResourceTagResource.resource_tag_resource_show": False,
    "ResourceTagResource.resource_tag_resource_create": False,
    "ResourceTagResource.resource_tag_resource_modify": False,
    "ResourceTagResource.resource_tag_resource_delete": False,
}


class ResourceTagResourceSchema(ResourceSchema, metaclass=ResourceSchemaMeta):
    """The fields of the ResourceTagResource object"""

    href = fields.Str(
        data_key="href",
    )
    r""" This property provides the address in the API at which the tagged resource is available. Additional queries can be made on this
endpoint to fetch the resource's properties."""

    label = fields.Str(
        data_key="label",
    )
    r""" This is a human-readable classifier representing the type of thing that is pointed to by the href.

Example: volume"""

    svm = fields.Nested("netapp_ontap.resources.svm.SvmSchema", data_key="svm", unknown=EXCLUDE)
    r""" The svm field of the resource_tag_resource."""

    @property
    def resource(self):
        return ResourceTagResource

    gettable_fields = [
        "label",
        "svm.links",
        "svm.name",
        "svm.uuid",
    ]
    """label,svm.links,svm.name,svm.uuid,"""

    patchable_fields = [
    ]
    """"""

    postable_fields = [
        "href",
    ]
    """href,"""

def _get_field_list(field: str) -> Callable[[], List]:
    def getter():
        return [getattr(r, field) for r in ResourceTagResource.get_collection(fields=field)]
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
            raise NetAppRestError("ResourceTagResource modify job failed")
        if job.state == "success":
            break
        await asyncio.sleep(1)

class ResourceTagResource(Resource):
    r""" This object provides a pointer to the tagged resource in the API. Details about the tagged object are available by querying
the address of the href property. """

    _schema = ResourceTagResourceSchema
    _path = "/api/resource-tags/{resource_tag[value]}/resources"
    _keys = ["resource_tag.value", "href"]

    @classmethod
    def get_collection(
        cls,
        *args,
        connection: HostConnection = None,
        max_records: int = None,
        **kwargs
    ) -> Iterable["Resource"]:
        r"""Retrieves the resources for a specific tag
### Learn more
* [`DOC /resource-tags/{resource_tag.value}/resources`](#docs-cluster-resource-tags_{resource_tag.value}_resources)"""
        return super()._get_collection(*args, connection=connection, max_records=max_records, **kwargs)

    get_collection.__func__.__doc__ += "\n\n---\n" + inspect.cleandoc(Resource._get_collection.__doc__)

    if RECLINE_INSTALLED:
        @recline.command(name="resource tag resource show")
        def resource_tag_resource_show(
            resource_tag_value,
            href: Choices.define(_get_field_list("href"), cache_choices=True, inexact=True)=None,
            label: Choices.define(_get_field_list("label"), cache_choices=True, inexact=True)=None,
            fields: List[Choices.define(["href", "label", "*"])]=None,
        ) -> ResourceTable:
            """Fetch a list of ResourceTagResource resources

            Args:
                href: This property provides the address in the API at which the tagged resource is available. Additional queries can be made on this endpoint to fetch the resource's properties. 
                label: This is a human-readable classifier representing the type of thing that is pointed to by the href.
            """

            kwargs = {}
            if href is not None:
                kwargs["href"] = href
            if label is not None:
                kwargs["label"] = label
            if fields is not None:
                fields = ",".join(fields)
                kwargs["fields"] = fields

            return ResourceTagResource.get_collection(
                resource_tag_value,
                **kwargs
            )

    @classmethod
    def count_collection(
        cls,
        *args,
        connection: HostConnection = None,
        **kwargs
    ) -> int:
        """Returns a count of all ResourceTagResource resources that match the provided query"""
        return super()._count_collection(*args, connection=connection, **kwargs)

    count_collection.__func__.__doc__ += "\n\n---\n" + inspect.cleandoc(Resource._count_collection.__doc__)


    @classmethod
    def post_collection(
        cls,
        records: Iterable["ResourceTagResource"],
        *args,
        hydrate: bool = False,
        poll: bool = True,
        poll_interval: Optional[int] = None,
        poll_timeout: Optional[int] = None,
        connection: HostConnection = None,
        **kwargs
    ) -> Union[List["ResourceTagResource"], NetAppResponse]:
        r"""Creates a new tag on a specific resource.
### Learn more
* [`DOC /resource-tags/{resource_tag.value}/resources`](#docs-cluster-resource-tags_{resource_tag.value}_resources)"""
        return super()._post_collection(
            records, *args, hydrate=hydrate, poll=poll, poll_interval=poll_interval,
            poll_timeout=poll_timeout, connection=connection, **kwargs
        )

    post_collection.__func__.__doc__ += "\n\n---\n" + inspect.cleandoc(Resource._post_collection.__doc__)

    @classmethod
    def delete_collection(
        cls,
        *args,
        records: Iterable["ResourceTagResource"] = None,
        body: Union[Resource, dict] = None,
        poll: bool = True,
        poll_interval: Optional[int] = None,
        poll_timeout: Optional[int] = None,
        connection: HostConnection = None,
        **kwargs
    ) -> NetAppResponse:
        r"""Deletes a specific tag on a specific resource.
### Learn more
* [`DOC /resource-tags/{resource_tag.value}/resources`](#docs-cluster-resource-tags_{resource_tag.value}_resources)"""
        return super()._delete_collection(
            *args, body=body, records=records, poll=poll, poll_interval=poll_interval,
            poll_timeout=poll_timeout, connection=connection, **kwargs
        )

    delete_collection.__func__.__doc__ += "\n\n---\n" + inspect.cleandoc(Resource._delete_collection.__doc__)

    @classmethod
    def find(cls, *args, connection: HostConnection = None, **kwargs) -> Resource:
        r"""Retrieves the resources for a specific tag
### Learn more
* [`DOC /resource-tags/{resource_tag.value}/resources`](#docs-cluster-resource-tags_{resource_tag.value}_resources)"""
        return super()._find(*args, connection=connection, **kwargs)

    find.__func__.__doc__ += "\n\n---\n" + inspect.cleandoc(Resource._find.__doc__)

    def get(self, **kwargs) -> NetAppResponse:
        r"""Retrieves a specific resource for a specific tag.
### Learn more
* [`DOC /resource-tags/{resource_tag.value}/resources`](#docs-cluster-resource-tags_{resource_tag.value}_resources)"""
        return super()._get(**kwargs)

    get.__doc__ += "\n\n---\n" + inspect.cleandoc(Resource._get.__doc__)

    def post(
        self,
        hydrate: bool = False,
        poll: bool = True,
        poll_interval: Optional[int] = None,
        poll_timeout: Optional[int] = None,
        **kwargs
    ) -> NetAppResponse:
        r"""Creates a new tag on a specific resource.
### Learn more
* [`DOC /resource-tags/{resource_tag.value}/resources`](#docs-cluster-resource-tags_{resource_tag.value}_resources)"""
        return super()._post(
            hydrate=hydrate, poll=poll, poll_interval=poll_interval,
            poll_timeout=poll_timeout, **kwargs
        )

    post.__doc__ += "\n\n---\n" + inspect.cleandoc(Resource._post.__doc__)

    if RECLINE_INSTALLED:
        @recline.command(name="resource tag resource create")
        async def resource_tag_resource_create(
            resource_tag_value,
            href: str = None,
            label: str = None,
            svm: dict = None,
        ) -> ResourceTable:
            """Create an instance of a ResourceTagResource resource

            Args:
                href: This property provides the address in the API at which the tagged resource is available. Additional queries can be made on this endpoint to fetch the resource's properties. 
                label: This is a human-readable classifier representing the type of thing that is pointed to by the href.
                svm: 
            """

            kwargs = {}
            if href is not None:
                kwargs["href"] = href
            if label is not None:
                kwargs["label"] = label
            if svm is not None:
                kwargs["svm"] = svm

            resource = ResourceTagResource(
                resource_tag_value,
                **kwargs
            )
            try:
                response = resource.post(hydrate=True, poll=False)
                await _wait_for_job(response)
                resource.get()
            except NetAppRestError as err:
                raise ReclineCommandError("Unable to create ResourceTagResource: %s" % err)
            return [resource]


    def delete(
        self,
        body: Union[Resource, dict] = None,
        poll: bool = True,
        poll_interval: Optional[int] = None,
        poll_timeout: Optional[int] = None,
        **kwargs
    ) -> NetAppResponse:
        r"""Deletes a specific tag on a specific resource.
### Learn more
* [`DOC /resource-tags/{resource_tag.value}/resources`](#docs-cluster-resource-tags_{resource_tag.value}_resources)"""
        return super()._delete(
            body=body, poll=poll, poll_interval=poll_interval,
            poll_timeout=poll_timeout, **kwargs
        )

    delete.__doc__ += "\n\n---\n" + inspect.cleandoc(Resource._delete.__doc__)

    if RECLINE_INSTALLED:
        @recline.command(name="resource tag resource delete")
        async def resource_tag_resource_delete(
            resource_tag_value,
            href: str = None,
            label: str = None,
        ) -> None:
            """Delete an instance of a ResourceTagResource resource

            Args:
                href: This property provides the address in the API at which the tagged resource is available. Additional queries can be made on this endpoint to fetch the resource's properties. 
                label: This is a human-readable classifier representing the type of thing that is pointed to by the href.
            """

            kwargs = {}
            if href is not None:
                kwargs["href"] = href
            if label is not None:
                kwargs["label"] = label

            if hasattr(ResourceTagResource, "find"):
                resource = ResourceTagResource.find(
                    resource_tag_value,
                    **kwargs
                )
            else:
                resource = ResourceTagResource(resource_tag_value,)
            try:
                response = resource.delete(poll=False)
                await _wait_for_job(response)
            except NetAppRestError as err:
                raise ReclineCommandError("Unable to delete ResourceTagResource: %s" % err)


