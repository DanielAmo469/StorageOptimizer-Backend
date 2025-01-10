r"""
Copyright &copy; 2023 NetApp Inc.
All rights reserved.

This file has been automatically generated based on the ONTAP REST API documentation.

## Overview
Retrieves the key manager keys on the give node. The following operations are supported:

* Get
## Examples
### Retrieving key manager key-id information for a node
The following example shows how to retrieve key-ids present on a node for a key manager.
```python
from netapp_ontap import HostConnection
from netapp_ontap.resources import KeyManagerKeys

with HostConnection("<mgmt-ip>", username="admin", password="password", verify=False):
    resource = KeyManagerKeys(
        "f4f98a48-8a5c-c548-cd03-c6335f5803a8",
        "00000000-0000-0000-0000-000000000000",
        key_id="000000000000000002000000000005009ad4da8fea2cafe2bed803078b780ebe0000000000000c01",
    )
    resource.get()
    print(resource)

```
<div class="try_it_out">
<input id="example0_try_it_out" type="checkbox", class="try_it_out_check">
<label for="example0_try_it_out" class="try_it_out_button">Try it out</label>
<div id="example0_result" class="try_it_out_content">
```
KeyManagerKeys(
    {
        "restored": False,
        "key_manager": "onboard",
        "_links": {
            "self": {
                "href": "/api/security/key-managers/f4f98a48-8a5c-c548-cd03-c6335f5803a8/keys/00000000-0000-0000-0000-000000000000/key-ids/000000000000000002000000000005009ad4da8fea2cafe2bed803078b780ebe0000000000000c01"
            }
        },
        "key_id": "000000000000000002000000000005009ad4da8fea2cafe2bed803078b780ebe0000000000000c01",
        "key_store": "onboard",
        "key_tag": "4d82cc07-f9e0-114e-55dc-82a224bea631",
        "node": {"uuid": "00000000-0000-0000-0000-000000000000"},
        "key_user": "datavs",
        "key_store_type": "okm",
        "key_type": "vek",
        "encryption_algorithm": "XTS-AES-256",
    }
)

```
</div>
</div>

---
### Retrieving key manager key-id information of a specific key-type for a node
The following example shows how to retrieve key-ids of a specific key-type present on a node for a key manager.
```
# The API:
GET /api/security/key-manager/{security_key_manager.uuid}/keys/{node.uuid}/key-ids
# The call:
curl -X GET "https://<mgmt-ip>/api/security/key-managers/7c179931-044b-11ed-b7cd-005056bbc535/keys/44dac31e-0449-11ed-b7cd-005056bbc535/key-ids?key_type=nse_ak&return_records=true&return_timeout=15" -H "accept: application/json"
# The response:
  {
    "records": [
      {
        "key_server": "10.225.89.34:5696",
        "key_id": "000000000000000002000000000001003d5c5f8c497e8e36aa80566e08749a3d0000000000000000",
        "key_type": "nse_ak"
      },
      {
        "key_server": "10.225.89.34:5696",
        "key_id": "00000000000000000200000000000100c2dce9a3a15aeb8480db8d49c17d056c0000000000000000",
        "key_type": "nse_ak"
      }
    ],
    "num_records": 2
  }"""

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


__all__ = ["KeyManagerKeys", "KeyManagerKeysSchema"]
__pdoc__ = {
    "KeyManagerKeysSchema.resource": False,
    "KeyManagerKeysSchema.opts": False,
    "KeyManagerKeys.key_manager_keys_show": False,
    "KeyManagerKeys.key_manager_keys_create": False,
    "KeyManagerKeys.key_manager_keys_modify": False,
    "KeyManagerKeys.key_manager_keys_delete": False,
}


class KeyManagerKeysSchema(ResourceSchema, metaclass=ResourceSchemaMeta):
    """The fields of the KeyManagerKeys object"""

    links = fields.Nested("netapp_ontap.models.self_link.SelfLinkSchema", data_key="_links", unknown=EXCLUDE)
    r""" The links field of the key_manager_keys."""

    crn = fields.Str(
        data_key="crn",
    )
    r""" Cloud resource name.

Example: CRN=v1:bluemix:public:containers-kubernetes"""

    encryption_algorithm = fields.Str(
        data_key="encryption_algorithm",
    )
    r""" Encryption algorithm for the key

Example: XTS-AES-256"""

    key_id = fields.Str(
        data_key="key_id",
    )
    r""" Key identifier.

Example: 000000000000000002000000000001008963c9213194c59555c1bec8db3603c800000000"""

    key_manager = fields.Str(
        data_key="key_manager",
    )
    r""" Key manager key server managing the key. Indicates the external key server when external key manager is configured.

Example: keyserver1.local:5696"""

    key_server = fields.Str(
        data_key="key_server",
    )
    r""" External key server for key management.

Example: keyserver1.com:5698"""

    key_store = fields.Str(
        data_key="key_store",
        validate=enum_validation(['onboard', 'external']),
    )
    r""" Security key manager configured for the given key manager UUID. Key manager keystore value can be onboard or external.

Valid choices:

* onboard
* external"""

    key_store_type = fields.Str(
        data_key="key_store_type",
        validate=enum_validation(['okm', 'kmip', 'akv', 'unset', 'gcp', 'aws', 'ikp']),
    )
    r""" Security key manager keystore type. Keystore type can be onboard, external, or supported cloud key manager.

Valid choices:

* okm
* kmip
* akv
* unset
* gcp
* aws
* ikp"""

    key_tag = fields.Str(
        data_key="key_tag",
    )
    r""" Additional information associated with the key.

Example: key#"""

    key_type = fields.Str(
        data_key="key_type",
        validate=enum_validation(['nse_ak', 'aek', 'vek', 'nek', 'svm_kek']),
    )
    r""" Encryption Key type.

Valid choices:

* nse_ak
* aek
* vek
* nek
* svm_kek"""

    key_user = fields.Str(
        data_key="key_user",
    )
    r""" SVM associated with the key.

Example: vs1"""

    node = fields.Nested("netapp_ontap.resources.node.NodeSchema", data_key="node", unknown=EXCLUDE)
    r""" The node field of the key_manager_keys."""

    policy = fields.Str(
        data_key="policy",
    )
    r""" Key store policy.

Example: IBM_Key_Lore"""

    restored = fields.Boolean(
        data_key="restored",
    )
    r""" Indicates whether the key is present locally on the node.

Example: true"""

    @property
    def resource(self):
        return KeyManagerKeys

    gettable_fields = [
        "links",
        "crn",
        "encryption_algorithm",
        "key_id",
        "key_manager",
        "key_server",
        "key_store",
        "key_store_type",
        "key_tag",
        "key_type",
        "key_user",
        "node.links",
        "node.name",
        "node.uuid",
        "policy",
        "restored",
    ]
    """links,crn,encryption_algorithm,key_id,key_manager,key_server,key_store,key_store_type,key_tag,key_type,key_user,node.links,node.name,node.uuid,policy,restored,"""

    patchable_fields = [
    ]
    """"""

    postable_fields = [
    ]
    """"""

def _get_field_list(field: str) -> Callable[[], List]:
    def getter():
        return [getattr(r, field) for r in KeyManagerKeys.get_collection(fields=field)]
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
            raise NetAppRestError("KeyManagerKeys modify job failed")
        if job.state == "success":
            break
        await asyncio.sleep(1)

class KeyManagerKeys(Resource):
    r""" Displays the keys stored in a key manager. """

    _schema = KeyManagerKeysSchema
    _path = "/api/security/key-managers/{security_key_manager[uuid]}/keys/{node[uuid]}/key-ids"
    _keys = ["security_key_manager.uuid", "node.uuid", "key_id"]

    @classmethod
    def get_collection(
        cls,
        *args,
        connection: HostConnection = None,
        max_records: int = None,
        **kwargs
    ) -> Iterable["Resource"]:
        r"""Retrieves key manager configurations.
### Required properties
* `security_key_manager.uuid` - Key manager UUID.
* `node.uuid` - Node UUID.
### Related ONTAP commands
* `security key-manager key query`

### Learn more
* [`DOC /security/key-managers/{security_key_manager.uuid}/keys/{node.uuid}/key-ids`](#docs-security-security_key-managers_{security_key_manager.uuid}_keys_{node.uuid}_key-ids)"""
        return super()._get_collection(*args, connection=connection, max_records=max_records, **kwargs)

    get_collection.__func__.__doc__ += "\n\n---\n" + inspect.cleandoc(Resource._get_collection.__doc__)

    if RECLINE_INSTALLED:
        @recline.command(name="key manager keys show")
        def key_manager_keys_show(
            node_uuid,
            security_key_manager_uuid,
            crn: Choices.define(_get_field_list("crn"), cache_choices=True, inexact=True)=None,
            encryption_algorithm: Choices.define(_get_field_list("encryption_algorithm"), cache_choices=True, inexact=True)=None,
            key_id: Choices.define(_get_field_list("key_id"), cache_choices=True, inexact=True)=None,
            key_manager: Choices.define(_get_field_list("key_manager"), cache_choices=True, inexact=True)=None,
            key_server: Choices.define(_get_field_list("key_server"), cache_choices=True, inexact=True)=None,
            key_store: Choices.define(_get_field_list("key_store"), cache_choices=True, inexact=True)=None,
            key_store_type: Choices.define(_get_field_list("key_store_type"), cache_choices=True, inexact=True)=None,
            key_tag: Choices.define(_get_field_list("key_tag"), cache_choices=True, inexact=True)=None,
            key_type: Choices.define(_get_field_list("key_type"), cache_choices=True, inexact=True)=None,
            key_user: Choices.define(_get_field_list("key_user"), cache_choices=True, inexact=True)=None,
            policy: Choices.define(_get_field_list("policy"), cache_choices=True, inexact=True)=None,
            restored: Choices.define(_get_field_list("restored"), cache_choices=True, inexact=True)=None,
            fields: List[Choices.define(["crn", "encryption_algorithm", "key_id", "key_manager", "key_server", "key_store", "key_store_type", "key_tag", "key_type", "key_user", "policy", "restored", "*"])]=None,
        ) -> ResourceTable:
            """Fetch a list of KeyManagerKeys resources

            Args:
                crn: Cloud resource name.
                encryption_algorithm: Encryption algorithm for the key
                key_id: Key identifier.
                key_manager: Key manager key server managing the key. Indicates the external key server when external key manager is configured.
                key_server: External key server for key management.
                key_store: Security key manager configured for the given key manager UUID. Key manager keystore value can be onboard or external.
                key_store_type: Security key manager keystore type. Keystore type can be onboard, external, or supported cloud key manager.
                key_tag: Additional information associated with the key.
                key_type: Encryption Key type.
                key_user: SVM associated with the key.
                policy: Key store policy.
                restored: Indicates whether the key is present locally on the node.
            """

            kwargs = {}
            if crn is not None:
                kwargs["crn"] = crn
            if encryption_algorithm is not None:
                kwargs["encryption_algorithm"] = encryption_algorithm
            if key_id is not None:
                kwargs["key_id"] = key_id
            if key_manager is not None:
                kwargs["key_manager"] = key_manager
            if key_server is not None:
                kwargs["key_server"] = key_server
            if key_store is not None:
                kwargs["key_store"] = key_store
            if key_store_type is not None:
                kwargs["key_store_type"] = key_store_type
            if key_tag is not None:
                kwargs["key_tag"] = key_tag
            if key_type is not None:
                kwargs["key_type"] = key_type
            if key_user is not None:
                kwargs["key_user"] = key_user
            if policy is not None:
                kwargs["policy"] = policy
            if restored is not None:
                kwargs["restored"] = restored
            if fields is not None:
                fields = ",".join(fields)
                kwargs["fields"] = fields

            return KeyManagerKeys.get_collection(
                node_uuid,
                security_key_manager_uuid,
                **kwargs
            )

    @classmethod
    def count_collection(
        cls,
        *args,
        connection: HostConnection = None,
        **kwargs
    ) -> int:
        """Returns a count of all KeyManagerKeys resources that match the provided query"""
        return super()._count_collection(*args, connection=connection, **kwargs)

    count_collection.__func__.__doc__ += "\n\n---\n" + inspect.cleandoc(Resource._count_collection.__doc__)




    @classmethod
    def find(cls, *args, connection: HostConnection = None, **kwargs) -> Resource:
        r"""Retrieves key manager configurations.
### Required properties
* `security_key_manager.uuid` - Key manager UUID.
* `node.uuid` - Node UUID.
### Related ONTAP commands
* `security key-manager key query`

### Learn more
* [`DOC /security/key-managers/{security_key_manager.uuid}/keys/{node.uuid}/key-ids`](#docs-security-security_key-managers_{security_key_manager.uuid}_keys_{node.uuid}_key-ids)"""
        return super()._find(*args, connection=connection, **kwargs)

    find.__func__.__doc__ += "\n\n---\n" + inspect.cleandoc(Resource._find.__doc__)

    def get(self, **kwargs) -> NetAppResponse:
        r"""Retrieves the key management keys information for the specified key_id.
### Related ONTAP commands
* `security key-manager key query -key-id <key_id>`

### Learn more
* [`DOC /security/key-managers/{security_key_manager.uuid}/keys/{node.uuid}/key-ids`](#docs-security-security_key-managers_{security_key_manager.uuid}_keys_{node.uuid}_key-ids)"""
        return super()._get(**kwargs)

    get.__doc__ += "\n\n---\n" + inspect.cleandoc(Resource._get.__doc__)





