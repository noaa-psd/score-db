"""
Copyright 2022 NOAA
All rights reserved.

Unit tests for io_utils

"""
import os
import pathlib
import pytest
import json
from collections import namedtuple

import metric_types as mts
from metric_types import MetricTypeData, MetricType, MetricTypeRequest

from score_db_base import handle_request


PYTEST_CALLING_DIR = pathlib.Path(__file__).parent.resolve()

MetricType = namedtuple(
    'MetricType',
    [
        'name',
        'type',
        'units',
        'stat_type',
        'description'
    ],
)

def test_validate_method():
    with pytest.raises(ValueError):
        mts.validate_method('blah')
    
    with pytest.raises(ValueError):
        mts.validate_method(None)
    
    with pytest.raises(ValueError):
        mts.validate_method([])
    
    for method in mts.VALID_METHODS:
        mts.validate_method(method)
    
    print(f'PYTEST_CALLING_DIR: {PYTEST_CALLING_DIR}')
    # print(f'EXPERIMENT_CONFIG_FILE: {EXPERIMENT_CONFIG_FILE}')

def test_parse_request_dict():

    description_temperature_count = {
        "details": "Innovation count statistics of temperature."
    }
    description_temperature_rmsd = {
        "details": "Innovation rmsd statistics of temperature."
    }
    description_temperature_bias = {
        "details": "Innovation bias statistics of temperature."
    }
    description_uvwind_rmsd = {
        "details": "Innovation rmsd statistics of uv wind."
    }
    description_uvwind_count = {
        "details": "Innovation count statistics of uv wind."
    }
    description_uvwind_bias = {
        "details": "Innovation bias statistics of uv wind."
    }
    description_spechumid_count = {
        "details": "Innovation count statistics of specific humidity."
    }
    description_spechumid_rmsd = {
        "details": "Innovation rmsd statistics of specific humidity."
    }
    description_spechumid_bias = {
        "details": "Innovation bias statistics of specific humidity."
    }
    description_salinity_count = {
        "details": "Innovation count statistics of salinity."
    }
    description_salinity_rmsd = {
        "details": "Innovation rmsd statistics of salinity."
    }
    description_salinity_bias = {
        "details": "Innovation bias statistics of salinity."
    }

    metric_types = [
        MetricType(
            'innov_stats_temperature_count',
            'temperature',
            'celsius',
            'count',
            description_temperature_count
        ),
        MetricType(
            'innov_stats_temperature_bias',
            'temperature',
            'celsius',
            'bias',
            description_temperature_bias
        ),
        MetricType(
            'innov_stats_temperature_rmsd',
            'temperature',
            'celsius',
            'rmsd',
            description_temperature_rmsd
        ),
        MetricType(
            'innov_stats_uvwind_rmsd',
            'uvwind',
            'kph',
            'rmsd',
            description_uvwind_rmsd
        ),
        MetricType(
            'innov_stats_uvwind_count',
            'uvwind',
            'kph',
            'count',
            description_uvwind_count
        ),
        MetricType(
            'innov_stats_uvwind_bias',
            'uvwind',
            'kph',
            'bias',
            description_uvwind_bias
        ),
        MetricType(
            'innov_stats_spechumid_rmsd',
            'spechumid',
            'grams of water vapor per cubic meter volume of air',
            'rmsd',
            description_spechumid_rmsd
        ),
        MetricType(
            'innov_stats_spechumid_count',
            'spechumid',
            'grams of water vapor per cubic meter volume of air',
            'count',
            description_spechumid_count
        ),
        MetricType(
            'innov_stats_spechumid_bias',
            'spechumid',
            'grams of water vapor per cubic meter volume of air',
            'bias',
            description_spechumid_bias
        ),
        MetricType(
            'innov_stats_salinity_bias',
            'salinity',
            'practical salinity',
            'bias',
            description_salinity_bias
        ),
        MetricType(
            'innov_stats_salinity_rmsd',
            'salinity',
            'practical salinity',
            'rmsd',
            description_temperature_rmsd
        )
    ]

    for m_type in metric_types:
        request_dict = {
            'name': 'metric_type',
            'method': 'PUT',
            'body': {
                'name': m_type.name,
                'measurement_type': m_type.type,
                # 'measurement_units': 'grams of water vapor per cubic meter volume of air',
                'measurement_units': m_type.units,
                'stat_type': m_type.stat_type,
                'description': json.dumps(m_type.description)
            }
        }

        mtr = MetricTypeRequest(request_dict)
        mtr.submit()

def test_send_get_request():

    request_dict = {
        'name': 'metric_type',
        'method': 'GET',
        'params': {
            'filters': {
                'name': {
                    # 'like': '%_3DVAR_%',
                    'exact': 'innov_stats_temperature_rmsd',
                    'in': ['innov_stats_temperature_rmsd', 'innov_stats_uvwind_rmsd']
                },
                'measurement_type': {
                    'exact': 'temperature'
                },
                'measurement_unit': {
                    'like': 'celsius'
                },
                'stat_type': {
                    'exact': 'rmsd'
                },
            },
            'ordering': [
                {'name': 'name', 'order_by': 'desc'},
                {'name': 'created_at', 'order_by': 'desc'}
            ],
            'record_limit': 4
        }
    }

    er = MetricTypeRequest(request_dict)
    er.submit()