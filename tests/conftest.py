"""Shared test fixtures with realistic CockroachDB range data."""

import pytest

SAMPLE_RANGE_DATA = {
    "ranges": {
        "1": {
            "rangeId": 1,
            "nodes": [
                {
                    "nodeId": 1,
                    "range": {
                        "span": {"startKey": "/Table/55/1", "endKey": "/Table/55/2"},
                        "state": {
                            "state": {
                                "lease": {"replica": {"nodeId": 1}},
                                "stats": {"liveCount": 50000},
                            }
                        },
                        "stats": {"queriesPerSecond": 1200.5, "writesPerSecond": 300.1},
                    },
                },
                {
                    "nodeId": 2,
                    "range": {
                        "span": {"startKey": "/Table/55/1", "endKey": "/Table/55/2"},
                        "state": {
                            "state": {
                                "lease": {"replica": {"nodeId": 1}},
                                "stats": {"liveCount": 50000},
                            }
                        },
                        "stats": {"queriesPerSecond": 0, "writesPerSecond": 0},
                    },
                },
            ],
        },
        "2": {
            "rangeId": 2,
            "nodes": [
                {
                    "nodeId": 2,
                    "range": {
                        "span": {"startKey": "/Table/55/2", "endKey": "/Table/55/3"},
                        "state": {
                            "state": {
                                "lease": {"replica": {"nodeId": 2}},
                                "stats": {"liveCount": 30000},
                            }
                        },
                        "stats": {"queriesPerSecond": 800.3, "writesPerSecond": 150.7},
                    },
                },
            ],
        },
        "3": {
            "rangeId": 3,
            "nodes": [
                {
                    "nodeId": 3,
                    "range": {
                        "span": {"startKey": "/Table/66/1", "endKey": "/Table/66/2"},
                        "state": {
                            "state": {
                                "lease": {"replica": {"nodeId": 3}},
                                "stats": {"liveCount": 10000},
                            }
                        },
                        "stats": {"queriesPerSecond": 5000.0, "writesPerSecond": 2000.0},
                    },
                },
            ],
        },
        "4": {
            "rangeId": 4,
            "nodes": [
                {
                    "nodeId": 1,
                    "range": {
                        "span": {"startKey": "/System/tsd", "endKey": "/System/tse"},
                        "state": {
                            "state": {
                                "lease": {"replica": {"nodeId": 1}},
                                "stats": {"liveCount": 5000},
                            }
                        },
                        "stats": {"queriesPerSecond": 50.0, "writesPerSecond": 10.0},
                    },
                },
            ],
        },
    }
}


@pytest.fixture
def sample_range_data():
    return SAMPLE_RANGE_DATA
