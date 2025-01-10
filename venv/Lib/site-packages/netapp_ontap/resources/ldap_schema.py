r"""
Copyright &copy; 2023 NetApp Inc.
All rights reserved.

This file has been automatically generated based on the ONTAP REST API documentation.

## Overview
ONTAP provides four default LDAP schemas. These are:

 * MS-AD-BIS
      Based on RFC-2307bis. This is the preferred LDAP schema for most standard Windows 2012 and later LDAP deployments.

 * AD-IDMU
      Based on Active Directory Identity Management for UNIX. This schema is appropriate for most Windows 2008, Windows 2012, and later AD servers.

 * AD-SFU
      Based on Active Directory Services for UNIX. This schema is appropriate for most Windows 2003 and earlier AD servers.

 * RFC-2307
      Based on RFC-2307 (an approach that uses LDAP as a network information service). This schema is appropriate for most UNIX AD servers.
## Examples
### Retrieving LDAP schema information
---
```python
from netapp_ontap import HostConnection
from netapp_ontap.resources import LdapSchema

with HostConnection("<mgmt-ip>", username="admin", password="password", verify=False):
    print(list(LdapSchema.get_collection()))

```
<div class="try_it_out">
<input id="example0_try_it_out" type="checkbox", class="try_it_out_check">
<label for="example0_try_it_out" class="try_it_out_button">Try it out</label>
<div id="example0_result" class="try_it_out_content">
```
[
    LdapSchema(
        {
            "_links": {
                "self": {
                    "href": "/api/name-services/ldap-schemas/eda950c6-0a0c-11ec-bfcf-0050568e9150/AD-IDMU"
                }
            },
            "name": "AD-IDMU",
            "owner": {
                "name": "athiraacluster-1",
                "_links": {
                    "self": {
                        "href": "/api/svm/svms/eda950c6-0a0c-11ec-bfcf-0050568e9150"
                    }
                },
                "uuid": "eda950c6-0a0c-11ec-bfcf-0050568e9150",
            },
        }
    ),
    LdapSchema(
        {
            "_links": {
                "self": {
                    "href": "/api/name-services/ldap-schemas/eda950c6-0a0c-11ec-bfcf-0050568e9150/AD-SFU"
                }
            },
            "name": "AD-SFU",
            "owner": {
                "name": "athiraacluster-1",
                "_links": {
                    "self": {
                        "href": "/api/svm/svms/eda950c6-0a0c-11ec-bfcf-0050568e9150"
                    }
                },
                "uuid": "eda950c6-0a0c-11ec-bfcf-0050568e9150",
            },
        }
    ),
    LdapSchema(
        {
            "_links": {
                "self": {
                    "href": "/api/name-services/ldap-schemas/eda950c6-0a0c-11ec-bfcf-0050568e9150/MS-AD-BIS"
                }
            },
            "name": "MS-AD-BIS",
            "owner": {
                "name": "athiraacluster-1",
                "_links": {
                    "self": {
                        "href": "/api/svm/svms/eda950c6-0a0c-11ec-bfcf-0050568e9150"
                    }
                },
                "uuid": "eda950c6-0a0c-11ec-bfcf-0050568e9150",
            },
        }
    ),
    LdapSchema(
        {
            "_links": {
                "self": {
                    "href": "/api/name-services/ldap-schemas/eda950c6-0a0c-11ec-bfcf-0050568e9150/RFC-2307"
                }
            },
            "name": "RFC-2307",
            "owner": {
                "name": "athiraacluster-1",
                "_links": {
                    "self": {
                        "href": "/api/svm/svms/eda950c6-0a0c-11ec-bfcf-0050568e9150"
                    }
                },
                "uuid": "eda950c6-0a0c-11ec-bfcf-0050568e9150",
            },
        }
    ),
]

```
</div>
</div>

---
### Retrieving LDAP schema information for a given SVM and "name"
---
```python
from netapp_ontap import HostConnection
from netapp_ontap.resources import LdapSchema

with HostConnection("<mgmt-ip>", username="admin", password="password", verify=False):
    resource = LdapSchema(
        name="RFC-2307", **{"owner.uuid": "eda950c6-0a0c-11ec-bfcf-0050568e9150"}
    )
    resource.get()
    print(resource)

```
<div class="try_it_out">
<input id="example1_try_it_out" type="checkbox", class="try_it_out_check">
<label for="example1_try_it_out" class="try_it_out_button">Try it out</label>
<div id="example1_result" class="try_it_out_content">
```
LdapSchema(
    {
        "scope": "cluster",
        "_links": {
            "self": {
                "href": "/api/name-services/ldap-schemas/eda950c6-0a0c-11ec-bfcf-0050568e9150/RFC-2307"
            }
        },
        "name_mapping": {
            "account": {"windows": "windowsAccount", "unix": "unixAccount"},
            "windows_to_unix": {
                "object_class": "posixAccount",
                "attribute": "windowsAccount",
                "no_domain_prefix": False,
            },
        },
        "rfc2307": {
            "cn": {"group": "cn", "netgroup": "cn"},
            "posix": {"account": "posixAccount", "group": "posixGroup"},
            "nis": {
                "netgroup_triple": "nisNetgroupTriple",
                "object": "nisObject",
                "mapname": "nisMapName",
                "mapentry": "nisMapEntry",
                "netgroup": "nisNetgroup",
            },
            "member": {"uid": "memberUid", "nis_netgroup": "memberNisNetgroup"},
            "attribute": {
                "home_directory": "homeDirectory",
                "uid": "uid",
                "login_shell": "loginShell",
                "gecos": "gecos",
                "gid_number": "gidNumber",
                "uid_number": "uidNumber",
                "user_password": "userPassword",
            },
        },
        "global_schema": True,
        "rfc2307bis": {
            "maximum_groups": 256,
            "enabled": False,
            "group_of_unique_names": "groupOfUniqueNames",
            "unique_member": "uniqueMember",
        },
        "name": "RFC-2307",
        "comment": "Schema based on RFC 2307 (read-only)",
        "owner": {
            "name": "svm1",
            "_links": {
                "self": {"href": "/api/svm/svms/eda950c6-0a0c-11ec-bfcf-0050568e9150"}
            },
            "uuid": "eda950c6-0a0c-11ec-bfcf-0050568e9150",
        },
    }
)

```
</div>
</div>

---
### Creating an LDAP schema
---
```python
from netapp_ontap import HostConnection
from netapp_ontap.resources import LdapSchema

with HostConnection("<mgmt-ip>", username="admin", password="password", verify=False):
    resource = LdapSchema()
    resource.name = "schema"
    resource.template = {"name": "AD-IDMU"}
    resource.owner = {"uuid": "52ba8197-0a23-11ec-9622-0050568e9150", "name": "svm1"}
    resource.post(hydrate=True)
    print(resource)

```

---
### Updating an LDAP schema
---
```python
from netapp_ontap import HostConnection
from netapp_ontap.resources import LdapSchema

with HostConnection("<mgmt-ip>", username="admin", password="password", verify=False):
    resource = LdapSchema(
        name="schema", **{"owner.uuid": "52ba8197-0a23-11ec-9622-0050568e9150"}
    )
    resource.comment = "This is a comment for schema"
    resource.patch()

```

---
### Deleting an LDAP schema
---
```python
from netapp_ontap import HostConnection
from netapp_ontap.resources import LdapSchema

with HostConnection("<mgmt-ip>", username="admin", password="password", verify=False):
    resource = LdapSchema(
        name="schema", **{"owner.uuid": "52ba8197-0a23-11ec-9622-0050568e9150"}
    )
    resource.delete()

```

---"""

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


__all__ = ["LdapSchema", "LdapSchemaSchema"]
__pdoc__ = {
    "LdapSchemaSchema.resource": False,
    "LdapSchemaSchema.opts": False,
    "LdapSchema.ldap_schema_show": False,
    "LdapSchema.ldap_schema_create": False,
    "LdapSchema.ldap_schema_modify": False,
    "LdapSchema.ldap_schema_delete": False,
}


class LdapSchemaSchema(ResourceSchema, metaclass=ResourceSchemaMeta):
    """The fields of the LdapSchema object"""

    links = fields.Nested("netapp_ontap.models.self_link.SelfLinkSchema", data_key="_links", unknown=EXCLUDE)
    r""" The links field of the ldap_schema."""

    comment = fields.Str(
        data_key="comment",
    )
    r""" Comment to associate with the schema.

Example: Schema based on Active Directory Services for UNIX (read-only)."""

    global_schema = fields.Boolean(
        data_key="global_schema",
    )
    r""" A global schema that can be used by all the SVMs.

Example: true"""

    name = fields.Str(
        data_key="name",
        validate=len_validation(minimum=1, maximum=32),
    )
    r""" The name of the schema being created, modified or deleted.

Example: AD-SFU-v1"""

    name_mapping = fields.Nested("netapp_ontap.models.ldap_schema_name_mapping.LdapSchemaNameMappingSchema", data_key="name_mapping", unknown=EXCLUDE)
    r""" The name_mapping field of the ldap_schema."""

    owner = fields.Nested("netapp_ontap.resources.svm.SvmSchema", data_key="owner", unknown=EXCLUDE)
    r""" The owner field of the ldap_schema."""

    rfc2307 = fields.Nested("netapp_ontap.models.rfc2307.Rfc2307Schema", data_key="rfc2307", unknown=EXCLUDE)
    r""" The rfc2307 field of the ldap_schema."""

    rfc2307bis = fields.Nested("netapp_ontap.models.rfc2307bis.Rfc2307bisSchema", data_key="rfc2307bis", unknown=EXCLUDE)
    r""" The rfc2307bis field of the ldap_schema."""

    scope = fields.Str(
        data_key="scope",
        validate=enum_validation(['cluster', 'svm']),
    )
    r""" Scope of the entity. Set to "cluster" for cluster owned objects and to "svm" for SVM owned objects.

Valid choices:

* cluster
* svm"""

    template = fields.Nested("netapp_ontap.resources.ldap_schema.LdapSchemaSchema", data_key="template", unknown=EXCLUDE)
    r""" The template field of the ldap_schema."""

    @property
    def resource(self):
        return LdapSchema

    gettable_fields = [
        "links",
        "comment",
        "global_schema",
        "name",
        "name_mapping",
        "owner.links",
        "owner.name",
        "owner.uuid",
        "rfc2307",
        "rfc2307bis",
        "scope",
    ]
    """links,comment,global_schema,name,name_mapping,owner.links,owner.name,owner.uuid,rfc2307,rfc2307bis,scope,"""

    patchable_fields = [
        "comment",
        "name_mapping",
        "owner.name",
        "owner.uuid",
        "rfc2307",
        "rfc2307bis",
    ]
    """comment,name_mapping,owner.name,owner.uuid,rfc2307,rfc2307bis,"""

    postable_fields = [
        "name",
        "owner.name",
        "owner.uuid",
        "template.name",
    ]
    """name,owner.name,owner.uuid,template.name,"""

def _get_field_list(field: str) -> Callable[[], List]:
    def getter():
        return [getattr(r, field) for r in LdapSchema.get_collection(fields=field)]
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
            raise NetAppRestError("LdapSchema modify job failed")
        if job.state == "success":
            break
        await asyncio.sleep(1)

class LdapSchema(Resource):
    """Allows interaction with LdapSchema objects on the host"""

    _schema = LdapSchemaSchema
    _path = "/api/name-services/ldap-schemas"
    _keys = ["owner.uuid", "name"]

    @classmethod
    def get_collection(
        cls,
        *args,
        connection: HostConnection = None,
        max_records: int = None,
        **kwargs
    ) -> Iterable["Resource"]:
        r"""Retrieves all the LDAP schemas.
### Related ONTAP commands
* `vserver services name-service ldap client schema show`

### Learn more
* [`DOC /name-services/ldap-schemas`](#docs-name-services-name-services_ldap-schemas)"""
        return super()._get_collection(*args, connection=connection, max_records=max_records, **kwargs)

    get_collection.__func__.__doc__ += "\n\n---\n" + inspect.cleandoc(Resource._get_collection.__doc__)

    if RECLINE_INSTALLED:
        @recline.command(name="ldap schema show")
        def ldap_schema_show(
            fields: List[Choices.define(["comment", "global_schema", "name", "scope", "*"])]=None,
        ) -> ResourceTable:
            """Fetch a list of LdapSchema resources

            Args:
                comment: Comment to associate with the schema.
                global_schema: A global schema that can be used by all the SVMs.
                name: The name of the schema being created, modified or deleted.
                scope: Scope of the entity. Set to \"cluster\" for cluster owned objects and to \"svm\" for SVM owned objects.
            """

            kwargs = {}
            if comment is not None:
                kwargs["comment"] = comment
            if global_schema is not None:
                kwargs["global_schema"] = global_schema
            if name is not None:
                kwargs["name"] = name
            if scope is not None:
                kwargs["scope"] = scope
            if fields is not None:
                fields = ",".join(fields)
                kwargs["fields"] = fields

            return LdapSchema.get_collection(
                **kwargs
            )

    @classmethod
    def count_collection(
        cls,
        *args,
        connection: HostConnection = None,
        **kwargs
    ) -> int:
        """Returns a count of all LdapSchema resources that match the provided query"""
        return super()._count_collection(*args, connection=connection, **kwargs)

    count_collection.__func__.__doc__ += "\n\n---\n" + inspect.cleandoc(Resource._count_collection.__doc__)

    @classmethod
    def patch_collection(
        cls,
        body: dict,
        *args,
        records: Iterable["LdapSchema"] = None,
        poll: bool = True,
        poll_interval: Optional[int] = None,
        poll_timeout: Optional[int] = None,
        connection: HostConnection = None,
        **kwargs
    ) -> NetAppResponse:
        r"""Updates LDAP schema details for a given owner and schema.
### Important notes
* The default LDAP schemas provided by ONTAP cannot be modified.
* LDAP schemas can only be modified by the owner of the schema.
### Related ONTAP commands
* `vserver services name-service ldap client schema modify`

### Learn more
* [`DOC /name-services/ldap-schemas`](#docs-name-services-name-services_ldap-schemas)"""
        return super()._patch_collection(
            body, *args, records=records, poll=poll, poll_interval=poll_interval,
            poll_timeout=poll_timeout, connection=connection, **kwargs
        )

    patch_collection.__func__.__doc__ += "\n\n---\n" + inspect.cleandoc(Resource._patch_collection.__doc__)

    @classmethod
    def post_collection(
        cls,
        records: Iterable["LdapSchema"],
        *args,
        hydrate: bool = False,
        poll: bool = True,
        poll_interval: Optional[int] = None,
        poll_timeout: Optional[int] = None,
        connection: HostConnection = None,
        **kwargs
    ) -> Union[List["LdapSchema"], NetAppResponse]:
        r"""Creates an LDAP schema.
### Important notes
* To create a new schema, first create a copy of the default schemas provided by ONTAP and then modify the copy accordingly.
* If no value is specified for the owner.uuid or owner.name fields, the cserver UUID and name are used by default.
### Related ONTAP commands
* `vserver services name-service ldap client schema copy`

### Learn more
* [`DOC /name-services/ldap-schemas`](#docs-name-services-name-services_ldap-schemas)"""
        return super()._post_collection(
            records, *args, hydrate=hydrate, poll=poll, poll_interval=poll_interval,
            poll_timeout=poll_timeout, connection=connection, **kwargs
        )

    post_collection.__func__.__doc__ += "\n\n---\n" + inspect.cleandoc(Resource._post_collection.__doc__)

    @classmethod
    def delete_collection(
        cls,
        *args,
        records: Iterable["LdapSchema"] = None,
        body: Union[Resource, dict] = None,
        poll: bool = True,
        poll_interval: Optional[int] = None,
        poll_timeout: Optional[int] = None,
        connection: HostConnection = None,
        **kwargs
    ) -> NetAppResponse:
        r"""Deletes an existing schema.
### Related ONTAP commands
* `vserver services name-service ldap client schema delete`

### Learn more
* [`DOC /name-services/ldap-schemas`](#docs-name-services-name-services_ldap-schemas)"""
        return super()._delete_collection(
            *args, body=body, records=records, poll=poll, poll_interval=poll_interval,
            poll_timeout=poll_timeout, connection=connection, **kwargs
        )

    delete_collection.__func__.__doc__ += "\n\n---\n" + inspect.cleandoc(Resource._delete_collection.__doc__)

    @classmethod
    def find(cls, *args, connection: HostConnection = None, **kwargs) -> Resource:
        r"""Retrieves all the LDAP schemas.
### Related ONTAP commands
* `vserver services name-service ldap client schema show`

### Learn more
* [`DOC /name-services/ldap-schemas`](#docs-name-services-name-services_ldap-schemas)"""
        return super()._find(*args, connection=connection, **kwargs)

    find.__func__.__doc__ += "\n\n---\n" + inspect.cleandoc(Resource._find.__doc__)

    def get(self, **kwargs) -> NetAppResponse:
        r"""Retrieves LDAP schema details for a given owner and schema.
### Related ONTAP commands
* `vserver services name-service ldap client schema show`

### Learn more
* [`DOC /name-services/ldap-schemas`](#docs-name-services-name-services_ldap-schemas)"""
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
        r"""Creates an LDAP schema.
### Important notes
* To create a new schema, first create a copy of the default schemas provided by ONTAP and then modify the copy accordingly.
* If no value is specified for the owner.uuid or owner.name fields, the cserver UUID and name are used by default.
### Related ONTAP commands
* `vserver services name-service ldap client schema copy`

### Learn more
* [`DOC /name-services/ldap-schemas`](#docs-name-services-name-services_ldap-schemas)"""
        return super()._post(
            hydrate=hydrate, poll=poll, poll_interval=poll_interval,
            poll_timeout=poll_timeout, **kwargs
        )

    post.__doc__ += "\n\n---\n" + inspect.cleandoc(Resource._post.__doc__)

    if RECLINE_INSTALLED:
        @recline.command(name="ldap schema create")
        async def ldap_schema_create(
        ) -> ResourceTable:
            """Create an instance of a LdapSchema resource

            Args:
                links: 
                comment: Comment to associate with the schema.
                global_schema: A global schema that can be used by all the SVMs.
                name: The name of the schema being created, modified or deleted.
                name_mapping: 
                owner: 
                rfc2307: 
                rfc2307bis: 
                scope: Scope of the entity. Set to \"cluster\" for cluster owned objects and to \"svm\" for SVM owned objects.
                template: 
            """

            kwargs = {}
            if links is not None:
                kwargs["links"] = links
            if comment is not None:
                kwargs["comment"] = comment
            if global_schema is not None:
                kwargs["global_schema"] = global_schema
            if name is not None:
                kwargs["name"] = name
            if name_mapping is not None:
                kwargs["name_mapping"] = name_mapping
            if owner is not None:
                kwargs["owner"] = owner
            if rfc2307 is not None:
                kwargs["rfc2307"] = rfc2307
            if rfc2307bis is not None:
                kwargs["rfc2307bis"] = rfc2307bis
            if scope is not None:
                kwargs["scope"] = scope
            if template is not None:
                kwargs["template"] = template

            resource = LdapSchema(
                **kwargs
            )
            try:
                response = resource.post(hydrate=True, poll=False)
                await _wait_for_job(response)
                resource.get()
            except NetAppRestError as err:
                raise ReclineCommandError("Unable to create LdapSchema: %s" % err)
            return [resource]

    def patch(
        self,
        hydrate: bool = False,
        poll: bool = True,
        poll_interval: Optional[int] = None,
        poll_timeout: Optional[int] = None,
        **kwargs
    ) -> NetAppResponse:
        r"""Updates LDAP schema details for a given owner and schema.
### Important notes
* The default LDAP schemas provided by ONTAP cannot be modified.
* LDAP schemas can only be modified by the owner of the schema.
### Related ONTAP commands
* `vserver services name-service ldap client schema modify`

### Learn more
* [`DOC /name-services/ldap-schemas`](#docs-name-services-name-services_ldap-schemas)"""
        return super()._patch(
            hydrate=hydrate, poll=poll, poll_interval=poll_interval,
            poll_timeout=poll_timeout, **kwargs
        )

    patch.__doc__ += "\n\n---\n" + inspect.cleandoc(Resource._patch.__doc__)

    if RECLINE_INSTALLED:
        @recline.command(name="ldap schema modify")
        async def ldap_schema_modify(
        ) -> ResourceTable:
            """Modify an instance of a LdapSchema resource

            Args:
                comment: Comment to associate with the schema.
                query_comment: Comment to associate with the schema.
                global_schema: A global schema that can be used by all the SVMs.
                query_global_schema: A global schema that can be used by all the SVMs.
                name: The name of the schema being created, modified or deleted.
                query_name: The name of the schema being created, modified or deleted.
                scope: Scope of the entity. Set to \"cluster\" for cluster owned objects and to \"svm\" for SVM owned objects.
                query_scope: Scope of the entity. Set to \"cluster\" for cluster owned objects and to \"svm\" for SVM owned objects.
            """

            kwargs = {}
            changes = {}
            if query_comment is not None:
                kwargs["comment"] = query_comment
            if query_global_schema is not None:
                kwargs["global_schema"] = query_global_schema
            if query_name is not None:
                kwargs["name"] = query_name
            if query_scope is not None:
                kwargs["scope"] = query_scope

            if comment is not None:
                changes["comment"] = comment
            if global_schema is not None:
                changes["global_schema"] = global_schema
            if name is not None:
                changes["name"] = name
            if scope is not None:
                changes["scope"] = scope

            if hasattr(LdapSchema, "find"):
                resource = LdapSchema.find(
                    **kwargs
                )
            else:
                resource = LdapSchema()
            try:
                for key, value in changes.items():
                    setattr(resource, key, value)
                response = resource.patch(poll=False)
                await _wait_for_job(response)
                resource.get(fields=",".join(changes.keys()))
                return [resource]
            except NetAppRestError as err:
                raise ReclineCommandError("Unable to modify LdapSchema: %s" % err)

    def delete(
        self,
        body: Union[Resource, dict] = None,
        poll: bool = True,
        poll_interval: Optional[int] = None,
        poll_timeout: Optional[int] = None,
        **kwargs
    ) -> NetAppResponse:
        r"""Deletes an existing schema.
### Related ONTAP commands
* `vserver services name-service ldap client schema delete`

### Learn more
* [`DOC /name-services/ldap-schemas`](#docs-name-services-name-services_ldap-schemas)"""
        return super()._delete(
            body=body, poll=poll, poll_interval=poll_interval,
            poll_timeout=poll_timeout, **kwargs
        )

    delete.__doc__ += "\n\n---\n" + inspect.cleandoc(Resource._delete.__doc__)

    if RECLINE_INSTALLED:
        @recline.command(name="ldap schema delete")
        async def ldap_schema_delete(
        ) -> None:
            """Delete an instance of a LdapSchema resource

            Args:
                comment: Comment to associate with the schema.
                global_schema: A global schema that can be used by all the SVMs.
                name: The name of the schema being created, modified or deleted.
                scope: Scope of the entity. Set to \"cluster\" for cluster owned objects and to \"svm\" for SVM owned objects.
            """

            kwargs = {}
            if comment is not None:
                kwargs["comment"] = comment
            if global_schema is not None:
                kwargs["global_schema"] = global_schema
            if name is not None:
                kwargs["name"] = name
            if scope is not None:
                kwargs["scope"] = scope

            if hasattr(LdapSchema, "find"):
                resource = LdapSchema.find(
                    **kwargs
                )
            else:
                resource = LdapSchema()
            try:
                response = resource.delete(poll=False)
                await _wait_for_job(response)
            except NetAppRestError as err:
                raise ReclineCommandError("Unable to delete LdapSchema: %s" % err)


