"""Client Data Delivery — exceptions.

Each error names exactly one failure category so a caller (Airflow task,
CLI script, test) can distinguish "this client doesn't exist" from "this
client isn't entitled to this dataset" from "the extract itself failed a
validation gate" — never a single generic exception for all three.
"""


class UnknownClientError(Exception):
    """Raised when client_id has no entry in configs/client_entitlements.json."""


class UnknownDatasetError(Exception):
    """Raised when dataset_name has no entry in configs/delivery_datasets.json."""


class UnauthorizedDatasetError(Exception):
    """Raised when client_id's entitlement does not include dataset_name."""


class DeliveryValidationError(Exception):
    """Raised when any pre-delivery validation gate fails. The delivery is
    never written to outbox/ when this is raised — see src/delivery/pipeline.py
    run()'s use of a staging tmp dir, moved into outbox/ only after every gate
    passes."""
