r"""
Copyright &copy; 2023 NetApp Inc.
All rights reserved.

This file has been automatically generated based on the ONTAP REST API documentation.

## Overview
An S3 bucket lifecycle management rule is a list of objects. Each rule defines a set of actions to be performed on the object within the bucket.
### Adding a lifecycle management rule on a S3 bucket under an SVM
```python
from netapp_ontap import HostConnection
from netapp_ontap.resources import S3BucketLifecycleRule

with HostConnection("<mgmt-ip>", username="admin", password="netapp1!", verify=False):
    resource = S3BucketLifecycleRule(
        "259b4e78-2893-67ea-9785-890456bbbec4", "259b4e46-2893-67ea-9145-909456bbbec4"
    )
    resource.name = "rule1"
    resource.expiration = {"object_age_days": "1000"}
    resource.abort_incomplete_multipart_upload = {"after_initiation_days": 200}
    resource.object_filter = {"prefix": "obj1*/", "size_greater_than": "1000"}
    resource.post(hydrate=True, return_timeout=0)
    print(resource)

```
<div class="try_it_out">
<input id="example0_try_it_out" type="checkbox", class="try_it_out_check">
<label for="example0_try_it_out" class="try_it_out_button">Try it out</label>
<div id="example0_result" class="try_it_out_content">
```
S3BucketLifecycleRule(
    {
        "abort_incomplete_multipart_upload": {"after_initiation_days": 200},
        "expiration": {"object_age_days": 1000},
        "name": "rule1",
        "object_filter": {"size_greater_than": 1000, "prefix": "obj1*/"},
    }
)

```
</div>
</div>

### Updating a lifecycle management rule on a S3 bucket under an SVM
```python
from netapp_ontap import HostConnection
from netapp_ontap.resources import S3BucketLifecycleRule

with HostConnection("<mgmt-ip>", username="admin", password="netapp1!", verify=False):
    resource = S3BucketLifecycleRule(
        "259b4e78-2893-67ea-9785-890456bbbec4",
        "259b4e46-2893-67ea-9145-909456bbbec4",
        name="rule1",
    )
    resource.expiration = {"object_age_days": "3000"}
    resource.abort_incomplete_multipart_upload = {"after_initiation_days": "5000"}
    resource.patch(hydrate=True, return_timeout=0)

```

### Deleting a lifecycle management rule on a S3 bucket under an SVM
```python
from netapp_ontap import HostConnection
from netapp_ontap.resources import S3BucketLifecycleRule

with HostConnection("<mgmt-ip>", username="admin", password="password", verify=False):
    resource = S3BucketLifecycleRule(
        "259b4e78-2893-67ea-9785-890456bbbec4",
        "259b4e46-2893-67ea-9145-909456bbbec4",
        name="rule1",
    )
    resource.delete()

```

### To delete an action within a rule, pass null inside the action-object."""

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


__all__ = ["S3BucketLifecycleRule", "S3BucketLifecycleRuleSchema"]
__pdoc__ = {
    "S3BucketLifecycleRuleSchema.resource": False,
    "S3BucketLifecycleRuleSchema.opts": False,
    "S3BucketLifecycleRule.s3_bucket_lifecycle_rule_show": False,
    "S3BucketLifecycleRule.s3_bucket_lifecycle_rule_create": False,
    "S3BucketLifecycleRule.s3_bucket_lifecycle_rule_modify": False,
    "S3BucketLifecycleRule.s3_bucket_lifecycle_rule_delete": False,
}


class S3BucketLifecycleRuleSchema(ResourceSchema, metaclass=ResourceSchemaMeta):
    """The fields of the S3BucketLifecycleRule object"""

    links = fields.Nested("netapp_ontap.models.self_link.SelfLinkSchema", data_key="_links", unknown=EXCLUDE)
    r""" The links field of the s3_bucket_lifecycle_rule."""

    abort_incomplete_multipart_upload = fields.Nested("netapp_ontap.models.s3_bucket_lifecycle_abort_incomplete_multipart_upload.S3BucketLifecycleAbortIncompleteMultipartUploadSchema", data_key="abort_incomplete_multipart_upload", unknown=EXCLUDE)
    r""" The abort_incomplete_multipart_upload field of the s3_bucket_lifecycle_rule."""

    enabled = fields.Boolean(
        data_key="enabled",
    )
    r""" Specifies whether or not the associated rule is enabled."""

    expiration = fields.Nested("netapp_ontap.models.s3_bucket_lifecycle_expiration.S3BucketLifecycleExpirationSchema", data_key="expiration", unknown=EXCLUDE)
    r""" The expiration field of the s3_bucket_lifecycle_rule."""

    name = fields.Str(
        data_key="name",
    )
    r""" Bucket lifecycle management rule identifier."""

    non_current_version_expiration = fields.Nested("netapp_ontap.models.s3_bucket_lifecycle_non_current_version_expiration.S3BucketLifecycleNonCurrentVersionExpirationSchema", data_key="non_current_version_expiration", unknown=EXCLUDE)
    r""" The non_current_version_expiration field of the s3_bucket_lifecycle_rule."""

    object_filter = fields.Nested("netapp_ontap.models.s3_bucket_lifecycle_object_filter.S3BucketLifecycleObjectFilterSchema", data_key="object_filter", unknown=EXCLUDE)
    r""" The object_filter field of the s3_bucket_lifecycle_rule."""

    @property
    def resource(self):
        return S3BucketLifecycleRule

    gettable_fields = [
        "links",
        "abort_incomplete_multipart_upload",
        "enabled",
        "expiration",
        "name",
        "non_current_version_expiration",
        "object_filter",
    ]
    """links,abort_incomplete_multipart_upload,enabled,expiration,name,non_current_version_expiration,object_filter,"""

    patchable_fields = [
        "abort_incomplete_multipart_upload",
        "enabled",
        "expiration",
        "non_current_version_expiration",
    ]
    """abort_incomplete_multipart_upload,enabled,expiration,non_current_version_expiration,"""

    postable_fields = [
        "abort_incomplete_multipart_upload",
        "enabled",
        "expiration",
        "name",
        "non_current_version_expiration",
        "object_filter",
    ]
    """abort_incomplete_multipart_upload,enabled,expiration,name,non_current_version_expiration,object_filter,"""

def _get_field_list(field: str) -> Callable[[], List]:
    def getter():
        return [getattr(r, field) for r in S3BucketLifecycleRule.get_collection(fields=field)]
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
            raise NetAppRestError("S3BucketLifecycleRule modify job failed")
        if job.state == "success":
            break
        await asyncio.sleep(1)

class S3BucketLifecycleRule(Resource):
    r""" Information about the lifecycle management rule of a bucket. """

    _schema = S3BucketLifecycleRuleSchema
    _path = "/api/protocols/s3/services/{svm[uuid]}/buckets/{s3_bucket[uuid]}/rules"
    _keys = ["svm.uuid", "s3_bucket.uuid", "name"]

    @classmethod
    def get_collection(
        cls,
        *args,
        connection: HostConnection = None,
        max_records: int = None,
        **kwargs
    ) -> Iterable["Resource"]:
        r"""Retrieves all S3 Lifecycle rules associated with a bucket. Note that in order to retrieve S3 bucket rule parametes, the 'fields' option should be set to '**'.
### Related ONTAP commands
* `vserver object-store-server bucket lifecycle-management-rule show`
### Learn more
* [`DOC /protocols/s3/services/{svm.uuid}/buckets/{s3_bucket.uuid}/rules`](#docs-object-store-protocols_s3_services_{svm.uuid}_buckets_{s3_bucket.uuid}_rules)
"""
        return super()._get_collection(*args, connection=connection, max_records=max_records, **kwargs)

    get_collection.__func__.__doc__ += "\n\n---\n" + inspect.cleandoc(Resource._get_collection.__doc__)

    if RECLINE_INSTALLED:
        @recline.command(name="s3 bucket lifecycle rule show")
        def s3_bucket_lifecycle_rule_show(
            s3_bucket_uuid,
            svm_uuid,
            enabled: Choices.define(_get_field_list("enabled"), cache_choices=True, inexact=True)=None,
            name: Choices.define(_get_field_list("name"), cache_choices=True, inexact=True)=None,
            fields: List[Choices.define(["enabled", "name", "*"])]=None,
        ) -> ResourceTable:
            """Fetch a list of S3BucketLifecycleRule resources

            Args:
                enabled: Specifies whether or not the associated rule is enabled.
                name: Bucket lifecycle management rule identifier.
            """

            kwargs = {}
            if enabled is not None:
                kwargs["enabled"] = enabled
            if name is not None:
                kwargs["name"] = name
            if fields is not None:
                fields = ",".join(fields)
                kwargs["fields"] = fields

            return S3BucketLifecycleRule.get_collection(
                s3_bucket_uuid,
                svm_uuid,
                **kwargs
            )

    @classmethod
    def count_collection(
        cls,
        *args,
        connection: HostConnection = None,
        **kwargs
    ) -> int:
        """Returns a count of all S3BucketLifecycleRule resources that match the provided query"""
        return super()._count_collection(*args, connection=connection, **kwargs)

    count_collection.__func__.__doc__ += "\n\n---\n" + inspect.cleandoc(Resource._count_collection.__doc__)

    @classmethod
    def patch_collection(
        cls,
        body: dict,
        *args,
        records: Iterable["S3BucketLifecycleRule"] = None,
        poll: bool = True,
        poll_interval: Optional[int] = None,
        poll_timeout: Optional[int] = None,
        connection: HostConnection = None,
        **kwargs
    ) -> NetAppResponse:
        r"""Updates the S3 bucket lifecycle rule configuration.
### Important notes
- The following fields can be modified for a bucket:
* `actions` - Lifecycle Management actions associated with the rule.
* `enabled` - Lifecycle Management rule is enabled or not..
* `object_age_days` - Number of days since creation after which objects can be deleted.
* `object_expiry_date` - Specific date from when objects can expire.
* `expired_object_delete_marker` - Cleanup object delete markers.
* `new_non_current_versions` - Number of latest non-current versions to be retained.
* `non_current_days` - Number of days after which non-current versions can be deleted.
* `after_initiation_days` - Number of days of initiation after which uploads can be aborted.
### Related ONTAP commands
* `vserver object-store-server bucket lifecycle-management-rule modify`
### Learn more
* [`DOC /protocols/s3/services/{svm.uuid}/buckets/{s3_bucket.uuid}/rules`](#docs-object-store-protocols_s3_services_{svm.uuid}_buckets_{s3_bucket.uuid}_rules)
"""
        return super()._patch_collection(
            body, *args, records=records, poll=poll, poll_interval=poll_interval,
            poll_timeout=poll_timeout, connection=connection, **kwargs
        )

    patch_collection.__func__.__doc__ += "\n\n---\n" + inspect.cleandoc(Resource._patch_collection.__doc__)

    @classmethod
    def post_collection(
        cls,
        records: Iterable["S3BucketLifecycleRule"],
        *args,
        hydrate: bool = False,
        poll: bool = True,
        poll_interval: Optional[int] = None,
        poll_timeout: Optional[int] = None,
        connection: HostConnection = None,
        **kwargs
    ) -> Union[List["S3BucketLifecycleRule"], NetAppResponse]:
        r"""Creates the S3 bucket lifecycle rule configuration.
### Required properties
* `name` - Lifecycle Management rule to be created.
* `actions` - Lifecycle Management actions associated with the rule.
### Recommended optional properties
* `enabled` - Lifecycle Management rule is enabled or not.
* `object_filter.prefix` - Lifecycle Management rule filter prefix.
* `object_filter.tags` - Lifecycle Management rule filter tags.
* `object_filter.size_greater_than` - Lifecycle Management rule filter minimum object size.
* `object_filter.size_less_than` - Lifecycle Management rule filter maximum object size.
* `object_age_days` - Number of days since creation after which objects can be deleted.
* `object_expiry_date` - Specific date from when objects can expire.
* `expired_object_delete_marker` - Cleanup object delete markers.
* `new_non_current_versions` - Number of latest non-current versions to be retained.
* `non_current_days` - Number of days after which non-current versions can be deleted.
* `after_initiation_days` - Number of days of initiation after which uploads can be aborted.
### Related ONTAP commands
* `vserver object-store-server bucket lifecycle-management-rule create`
### Learn more
* [`DOC /protocols/s3/services/{svm.uuid}/buckets/{s3_bucket.uuid}/rules`](#docs-object-store-protocols_s3_services_{svm.uuid}_buckets_{s3_bucket.uuid}_rules)
"""
        return super()._post_collection(
            records, *args, hydrate=hydrate, poll=poll, poll_interval=poll_interval,
            poll_timeout=poll_timeout, connection=connection, **kwargs
        )

    post_collection.__func__.__doc__ += "\n\n---\n" + inspect.cleandoc(Resource._post_collection.__doc__)

    @classmethod
    def delete_collection(
        cls,
        *args,
        records: Iterable["S3BucketLifecycleRule"] = None,
        body: Union[Resource, dict] = None,
        poll: bool = True,
        poll_interval: Optional[int] = None,
        poll_timeout: Optional[int] = None,
        connection: HostConnection = None,
        **kwargs
    ) -> NetAppResponse:
        r"""Deletes the S3 bucket lifecycle rule configuration.
### Related ONTAP commands
* `vserver object-store-server bucket lifecycle-management-rule delete`
### Learn more
* [`DOC /protocols/s3/services/{svm.uuid}/buckets/{s3_bucket.uuid}/rules`](#docs-object-store-protocols_s3_services_{svm.uuid}_buckets_{s3_bucket.uuid}_rules)
"""
        return super()._delete_collection(
            *args, body=body, records=records, poll=poll, poll_interval=poll_interval,
            poll_timeout=poll_timeout, connection=connection, **kwargs
        )

    delete_collection.__func__.__doc__ += "\n\n---\n" + inspect.cleandoc(Resource._delete_collection.__doc__)

    @classmethod
    def find(cls, *args, connection: HostConnection = None, **kwargs) -> Resource:
        r"""Retrieves all S3 Lifecycle rules associated with a bucket. Note that in order to retrieve S3 bucket rule parametes, the 'fields' option should be set to '**'.
### Related ONTAP commands
* `vserver object-store-server bucket lifecycle-management-rule show`
### Learn more
* [`DOC /protocols/s3/services/{svm.uuid}/buckets/{s3_bucket.uuid}/rules`](#docs-object-store-protocols_s3_services_{svm.uuid}_buckets_{s3_bucket.uuid}_rules)
"""
        return super()._find(*args, connection=connection, **kwargs)

    find.__func__.__doc__ += "\n\n---\n" + inspect.cleandoc(Resource._find.__doc__)

    def get(self, **kwargs) -> NetAppResponse:
        r"""Retrieves all S3 Lifecycle rules associated with a bucket. Note that in order to retrieve S3 bucket rule parametes, the 'fields' option should be set to '**'.
### Related ONTAP commands
* `vserver object-store-server bucket lifecycle-management-rule show`
### Learn more
* [`DOC /protocols/s3/services/{svm.uuid}/buckets/{s3_bucket.uuid}/rules`](#docs-object-store-protocols_s3_services_{svm.uuid}_buckets_{s3_bucket.uuid}_rules)
"""
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
        r"""Creates the S3 bucket lifecycle rule configuration.
### Required properties
* `name` - Lifecycle Management rule to be created.
* `actions` - Lifecycle Management actions associated with the rule.
### Recommended optional properties
* `enabled` - Lifecycle Management rule is enabled or not.
* `object_filter.prefix` - Lifecycle Management rule filter prefix.
* `object_filter.tags` - Lifecycle Management rule filter tags.
* `object_filter.size_greater_than` - Lifecycle Management rule filter minimum object size.
* `object_filter.size_less_than` - Lifecycle Management rule filter maximum object size.
* `object_age_days` - Number of days since creation after which objects can be deleted.
* `object_expiry_date` - Specific date from when objects can expire.
* `expired_object_delete_marker` - Cleanup object delete markers.
* `new_non_current_versions` - Number of latest non-current versions to be retained.
* `non_current_days` - Number of days after which non-current versions can be deleted.
* `after_initiation_days` - Number of days of initiation after which uploads can be aborted.
### Related ONTAP commands
* `vserver object-store-server bucket lifecycle-management-rule create`
### Learn more
* [`DOC /protocols/s3/services/{svm.uuid}/buckets/{s3_bucket.uuid}/rules`](#docs-object-store-protocols_s3_services_{svm.uuid}_buckets_{s3_bucket.uuid}_rules)
"""
        return super()._post(
            hydrate=hydrate, poll=poll, poll_interval=poll_interval,
            poll_timeout=poll_timeout, **kwargs
        )

    post.__doc__ += "\n\n---\n" + inspect.cleandoc(Resource._post.__doc__)

    if RECLINE_INSTALLED:
        @recline.command(name="s3 bucket lifecycle rule create")
        async def s3_bucket_lifecycle_rule_create(
            s3_bucket_uuid,
            svm_uuid,
            links: dict = None,
            abort_incomplete_multipart_upload: dict = None,
            enabled: bool = None,
            expiration: dict = None,
            name: str = None,
            non_current_version_expiration: dict = None,
            object_filter: dict = None,
        ) -> ResourceTable:
            """Create an instance of a S3BucketLifecycleRule resource

            Args:
                links: 
                abort_incomplete_multipart_upload: 
                enabled: Specifies whether or not the associated rule is enabled.
                expiration: 
                name: Bucket lifecycle management rule identifier.
                non_current_version_expiration: 
                object_filter: 
            """

            kwargs = {}
            if links is not None:
                kwargs["links"] = links
            if abort_incomplete_multipart_upload is not None:
                kwargs["abort_incomplete_multipart_upload"] = abort_incomplete_multipart_upload
            if enabled is not None:
                kwargs["enabled"] = enabled
            if expiration is not None:
                kwargs["expiration"] = expiration
            if name is not None:
                kwargs["name"] = name
            if non_current_version_expiration is not None:
                kwargs["non_current_version_expiration"] = non_current_version_expiration
            if object_filter is not None:
                kwargs["object_filter"] = object_filter

            resource = S3BucketLifecycleRule(
                s3_bucket_uuid,
                svm_uuid,
                **kwargs
            )
            try:
                response = resource.post(hydrate=True, poll=False)
                await _wait_for_job(response)
                resource.get()
            except NetAppRestError as err:
                raise ReclineCommandError("Unable to create S3BucketLifecycleRule: %s" % err)
            return [resource]

    def patch(
        self,
        hydrate: bool = False,
        poll: bool = True,
        poll_interval: Optional[int] = None,
        poll_timeout: Optional[int] = None,
        **kwargs
    ) -> NetAppResponse:
        r"""Updates the S3 bucket lifecycle rule configuration.
### Important notes
- The following fields can be modified for a bucket:
* `actions` - Lifecycle Management actions associated with the rule.
* `enabled` - Lifecycle Management rule is enabled or not..
* `object_age_days` - Number of days since creation after which objects can be deleted.
* `object_expiry_date` - Specific date from when objects can expire.
* `expired_object_delete_marker` - Cleanup object delete markers.
* `new_non_current_versions` - Number of latest non-current versions to be retained.
* `non_current_days` - Number of days after which non-current versions can be deleted.
* `after_initiation_days` - Number of days of initiation after which uploads can be aborted.
### Related ONTAP commands
* `vserver object-store-server bucket lifecycle-management-rule modify`
### Learn more
* [`DOC /protocols/s3/services/{svm.uuid}/buckets/{s3_bucket.uuid}/rules`](#docs-object-store-protocols_s3_services_{svm.uuid}_buckets_{s3_bucket.uuid}_rules)
"""
        return super()._patch(
            hydrate=hydrate, poll=poll, poll_interval=poll_interval,
            poll_timeout=poll_timeout, **kwargs
        )

    patch.__doc__ += "\n\n---\n" + inspect.cleandoc(Resource._patch.__doc__)

    if RECLINE_INSTALLED:
        @recline.command(name="s3 bucket lifecycle rule modify")
        async def s3_bucket_lifecycle_rule_modify(
            s3_bucket_uuid,
            svm_uuid,
            enabled: bool = None,
            query_enabled: bool = None,
            name: str = None,
            query_name: str = None,
        ) -> ResourceTable:
            """Modify an instance of a S3BucketLifecycleRule resource

            Args:
                enabled: Specifies whether or not the associated rule is enabled.
                query_enabled: Specifies whether or not the associated rule is enabled.
                name: Bucket lifecycle management rule identifier.
                query_name: Bucket lifecycle management rule identifier.
            """

            kwargs = {}
            changes = {}
            if query_enabled is not None:
                kwargs["enabled"] = query_enabled
            if query_name is not None:
                kwargs["name"] = query_name

            if enabled is not None:
                changes["enabled"] = enabled
            if name is not None:
                changes["name"] = name

            if hasattr(S3BucketLifecycleRule, "find"):
                resource = S3BucketLifecycleRule.find(
                    s3_bucket_uuid,
                    svm_uuid,
                    **kwargs
                )
            else:
                resource = S3BucketLifecycleRule(s3_bucket_uuid,svm_uuid,)
            try:
                for key, value in changes.items():
                    setattr(resource, key, value)
                response = resource.patch(poll=False)
                await _wait_for_job(response)
                resource.get(fields=",".join(changes.keys()))
                return [resource]
            except NetAppRestError as err:
                raise ReclineCommandError("Unable to modify S3BucketLifecycleRule: %s" % err)

    def delete(
        self,
        body: Union[Resource, dict] = None,
        poll: bool = True,
        poll_interval: Optional[int] = None,
        poll_timeout: Optional[int] = None,
        **kwargs
    ) -> NetAppResponse:
        r"""Deletes the S3 bucket lifecycle rule configuration.
### Related ONTAP commands
* `vserver object-store-server bucket lifecycle-management-rule delete`
### Learn more
* [`DOC /protocols/s3/services/{svm.uuid}/buckets/{s3_bucket.uuid}/rules`](#docs-object-store-protocols_s3_services_{svm.uuid}_buckets_{s3_bucket.uuid}_rules)
"""
        return super()._delete(
            body=body, poll=poll, poll_interval=poll_interval,
            poll_timeout=poll_timeout, **kwargs
        )

    delete.__doc__ += "\n\n---\n" + inspect.cleandoc(Resource._delete.__doc__)

    if RECLINE_INSTALLED:
        @recline.command(name="s3 bucket lifecycle rule delete")
        async def s3_bucket_lifecycle_rule_delete(
            s3_bucket_uuid,
            svm_uuid,
            enabled: bool = None,
            name: str = None,
        ) -> None:
            """Delete an instance of a S3BucketLifecycleRule resource

            Args:
                enabled: Specifies whether or not the associated rule is enabled.
                name: Bucket lifecycle management rule identifier.
            """

            kwargs = {}
            if enabled is not None:
                kwargs["enabled"] = enabled
            if name is not None:
                kwargs["name"] = name

            if hasattr(S3BucketLifecycleRule, "find"):
                resource = S3BucketLifecycleRule.find(
                    s3_bucket_uuid,
                    svm_uuid,
                    **kwargs
                )
            else:
                resource = S3BucketLifecycleRule(s3_bucket_uuid,svm_uuid,)
            try:
                response = resource.delete(poll=False)
                await _wait_for_job(response)
            except NetAppRestError as err:
                raise ReclineCommandError("Unable to delete S3BucketLifecycleRule: %s" % err)


