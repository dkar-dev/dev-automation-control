from __future__ import annotations

import uuid


def generate_opaque_id() -> str:
    return str(uuid.uuid4())


def generate_flow_id() -> str:
    return generate_opaque_id()


def generate_queue_item_id() -> str:
    return generate_opaque_id()


def generate_run_id() -> str:
    return generate_opaque_id()


def generate_state_transition_id() -> str:
    return generate_opaque_id()
