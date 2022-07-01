"""
Copyright 2022 NOAA
All rights reserved.

Unit tests for io_utils

"""
import os
import pathlib
import pytest
import json

import score_table_models as scr_models
from score_table_models import Experiment as experiments_table
import experiments as expts
from experiments import ExperimentData, Experiment, ExperimentRequest

from score_db_base import handle_request


PYTEST_CALLING_DIR = pathlib.Path(__file__).parent.resolve()
EXPERIMENT_CONFIG_FILE = os.path.join(
    PYTEST_CALLING_DIR,
    'configs',
    'experiment_description.json'
)



def test_validate_method():
    with pytest.raises(ValueError):
        expts.validate_method('blah')
    
    with pytest.raises(ValueError):
        expts.validate_method(None)
    
    with pytest.raises(ValueError):
        expts.validate_method([])
    
    for method in expts.VALID_METHODS:
        expts.validate_method(method)
    
    print(f'PYTEST_CALLING_DIR: {PYTEST_CALLING_DIR}')
    print(f'EXPERIMENT_CONFIG_FILE: {EXPERIMENT_CONFIG_FILE}')

# def test_parse_request_dict():

#     with open(EXPERIMENT_CONFIG_FILE, 'r') as config_file:
#         data=config_file.read()
    
#     description = json.loads(data)

#     request_dict = {
#         'name': 'experiment',
#         'method': 'PUT',
#         'body': {
#             'name': 'UFSRNR_GSI_SOCA_3DVAR_COUPLED_AWS_C5N18XL_122015',
#             'datestr_format': '%Y-%m-%d_%H:%M:%S',
#             'cycle_start': '2015-12-01_00:00:00',
#             'cycle_stop': '2016-02-01_00:00:00',
#             'owner_id': 'Steve.Lawrence@noaa.gov',
#             'group_id': 'bob',
#             'experiment_type': 'UFSRNR_GSI_SOCA_3DVAR_COUPLED_012016',
#             'platform': 'pw_awv1',
#             'wallclock_start': '2022-02-01_16:22:04',
#             'wallclock_end': 'None',
#             'description': json.dumps(description)
#         }
#     }

#     er = ExperimentRequest(request_dict)
#     er.submit()

def test_send_get_request():

    request_dict = {
        'name': 'experiment',
        'method': 'GET',
        'params': {
            'filters': {
                'name': {
                    # 'like': '%_3DVAR_%',
                    'exact': 'UFSRNR_GSI_SOCA_3DVAR_COUPLED_AWS_C5N18XL_122015'
                },
                'cycle_start': {
                    'from': '2015-01-01_00:00:00',
                    'to': '2018-01-01_00:00:00'
                },
                'cycle_stop': {
                    'from': '2015-01-01_00:00:00',
                    'to': '2018-01-01_00:00:00'
                },
                'owner_id': {
                    'exact': 'Steve.Lawrence@noaa.gov'
                },
                # 'group_id': {
                #     'exact': 'gsienkf'
                # },
                'experiment_type': {
                    'like': '%COUPLED%'
                },
                'platform': {
                    'exact': 'pw_awv1'
                },
                'wallclock_start': {
                    'from': '2022-01-01_00:00:00',
                    'to': '2022-07-01_00:00:00'
                },
                # 'wallclock_end': {
                #     'from': '2015-01-01_00:00:00',
                #     'to': '2022-05-01_00:00:00'
                # }

            },
            'ordering': [
                {'name': 'group_id', 'order_by': 'desc'},
                {'name': 'created_at', 'order_by': 'desc'}
            ],
            'record_limit': 4
        }
    }

    er = ExperimentRequest(request_dict)
    er.submit()