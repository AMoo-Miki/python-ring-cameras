"""Tests for ring_doorbell.util helpers."""

from ring_doorbell.util import get_detection_types, resolve_motion_subtype

# Trimmed from a real Battery Doorbell history entry: a package delivery ding
# also contains the courier's earlier human detection.
PACKAGE_HISTORY_ENTRY = {
    "kind": "motion",
    "cv_properties": {
        "person_detected": None,
        "detection_type": "package_delivery",
        "detection_types": [
            {"detection_type": "human", "verified_timestamps": [1791425211007]},
            {
                "detection_type": "package_delivery",
                "verified_timestamps": [1791425219778],
            },
        ],
    },
}


def test_resolve_motion_subtype():
    assert resolve_motion_subtype("human", "human") == "human"
    assert resolve_motion_subtype("vehicle", None) == "vehicle"
    assert resolve_motion_subtype("package_delivery", "package_delivery") == (
        "package_delivery"
    )
    assert resolve_motion_subtype(None, "package_delivery") == "package_delivery"
    assert resolve_motion_subtype("motion", "motion") == "other_motion"
    assert resolve_motion_subtype(None, None) == "other_motion"


def test_get_detection_types_history_shape():
    assert get_detection_types(PACKAGE_HISTORY_ENTRY) == ["human", "package_delivery"]


def test_get_detection_types_location_shape():
    entry = {"cv": PACKAGE_HISTORY_ENTRY["cv_properties"]}
    assert get_detection_types(entry) == ["human", "package_delivery"]


def test_get_detection_types_primary_only():
    entry = {"cv_properties": {"detection_type": "human", "detection_types": None}}
    assert get_detection_types(entry) == ["human"]


def test_get_detection_types_missing():
    assert get_detection_types({"kind": "on_demand", "cv_properties": None}) == []
    assert get_detection_types({}) == []
