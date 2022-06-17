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

    description_temperature = {
        "details": "Innovation rmse statistics of temperature."
    }
    description_uvwind = {
        "details": "Innovation rmse statistics of uv wind."
    }
    description_spechumid = {
        "details": "Innovation rmse statistics of specific humidity."
    }

    metric_types = [
        MetricType('innov_stats_temperature_rmse', 'temperature', 'celsius', description_temperature),
        MetricType(
            'innov_stats_uvwind_rmse',
            'uvwind',
            'kph',
            description_uvwind),
        MetricType(
            'innov_stats_spechumid_rmse',
            'spechumid',
            'grams of water vapor per cubic meter volume of air',
            description_spechumid
        ),

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
                'stat_type': 'rmse',
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
                    'exact': 'innov_stats_temperature_rmse',
                    'in': ['innov_stats_temperature_rmse', 'innov_stats_uvwind_rmse']
                },
                'measurement_type': {
                    'exact': 'temperature'
                },
                'measurement_unit': {
                    'like': 'celsius'
                },
                'stat_type': {
                    'exact': 'rmse'
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