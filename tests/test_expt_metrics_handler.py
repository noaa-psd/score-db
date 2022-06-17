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
from score_table_models import ExperimentMetric as exp_metrics_table
import experiments as expts
import expt_metrics
from experiments import ExperimentData, Experiment, ExperimentRequest
from expt_metrics import ExptMetricInputData, ExptMetric, ExptMetricRequest

from score_db_base import handle_request


PYTEST_CALLING_DIR = pathlib.Path(__file__).parent.resolve()
EXPERIMENT_CONFIG_FILE = os.path.join(
    PYTEST_CALLING_DIR,
    'configs',
    'experiment_description.json'
)

    # 'ExptMetricInputData',
    # [
    #     'name',
    #     'region_name',
    #     'elevation',
    #     'elevation_unit',
    #     'value',
    #     'time_valid'
    # ],


# def test_put_exp_metrics_request_dict():

#     request_dict = {
#         'name': 'expt_metrics',
#         'method': 'PUT',
#         'body': {
#             'expt_name': 'UFSRNR_GSI_SOCA_3DVAR_COUPLED_AZURE_HC44RS_122015',
#             'expt_wallclock_start': '2022-08-03_02:40:34',
#             'metrics': [
#                 ExptMetricInputData('innov_stats_temperature_rmse', 'global', '0', 'kpa', 2.6, '2015-12-02_06:00:00'),
#                 ExptMetricInputData('innov_stats_uvwind_rmse', 'tropics', '50', 'kpa', 2.8, '2015-12-02_06:00:00')
#             ],
#             'datestr_format': '%Y-%m-%d_%H:%M:%S'
#         }
#     }

#     emr = ExptMetricRequest(request_dict)
#     result = emr.submit()
#     print(f'Experiment metrics PUT result: {result}')

def test_send_get_request():

    request_dict = {
        'name': 'experiment',
        'method': 'GET',
        'params': {
            'datestr_format': '%Y-%m-%d_%H:%M:%S',
            'filters': {
                'experiments': {
                    'name': {
                        'exact': 'UFSRNR_GSI_SOCA_3DVAR_COUPLED_AZURE_HC44RS_122015',
                    },
                    'wallclock_start': {
                        'from': '2022-08-03_02:00:00',
                        'to': '2022-08-03_06:00:00'
                    }
                },
                # 'metric_types': {
                    # 'name': {
                        # 'exact': 'innov_stats_temperature_rmse'
                    # },
                # },
                # 'regions': {

                # },

                'time_valid': {
                    'from': '2015-01-01_00:00:00',
                    'to': '2016-01-03_00:00:00',
                },
            },
            'ordering': [
                {'name': 'created_at', 'order_by': 'desc'}
            ]
        }
    }

    emr = ExptMetricRequest(request_dict)
    emr.submit()