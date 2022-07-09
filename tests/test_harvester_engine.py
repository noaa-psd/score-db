"""
Copyright 2022 NOAA
All rights reserved.

Unit tests for io_utils

"""
import os
import pathlib
import pytest
import json
import yaml


from harvest_innov_stats import HarvestInnovStatsRequest
import score_table_models as scr_models
from score_table_models import Experiment as experiments_table
from score_table_models import ExperimentMetric as exp_metrics_table
import experiments as expts
import expt_metrics
from experiments import ExperimentData, Experiment, ExperimentRequest
from expt_metrics import ExptMetricInputData, ExptMetric, ExptMetricRequest

from score_db_base import handle_request

PYTEST_CALLING_DIR = pathlib.Path(__file__).parent.resolve()

CYCLES = [0, 21600, 43200, 64800]
BASE = '/home/slawrence/Development/experiment_data/'
EXP_NAME = 'uncoupled_c96_3dvar/'
GSI_3DVAR = 'gsi/'
SOCA_3DVAR = '%Y%m%d%H%M%S/post/soca/3dvar/'
NETCDF_HARVESTER_CONFIG__VALID = 'netcdf_harvester_config__valid.yaml'

def test_run_innov_stats_harvester_for_date_range():

    harvester_control_dict = {
        'db_request_name': 'harvest_innov_stats',
        'date_range': {
            'datetime_str': '%Y-%m-%d %H:%M:%S',
            'start': '2016-01-01 00:00:00',
            'end': '2016-01-31 18:00:00'
        },

        'files': [
            {
                'filepath': BASE + EXP_NAME + GSI_3DVAR,
                'filename': 'innov_stats.metric.%Y%m%d%H.nc',
                'cycles': CYCLES,
                'harvester': 'innov_stats_netcdf',
                'metrics': ['temperature','spechumid','uvwind'],
                'stats': ['bias', 'count', 'rmsd'],
                'elevation_unit': 'plevs'
            },
            # {
            #     'filepath': BASE + EXP_NAME + SOCA_3DVAR,
            #     'filename': 'innov_stats.metric.%Y%m%d%H%M%S.nc',
            #     'cycles': [CYCLES[1]],
            #     'harvester': 'innov_stats_netcdf',
            #     'metrics': ['temperature','salinity'],
            #     'stats': ['bias', 'rmsd'],
            #     'elevation_unit': 'depth'
            # },
        ],
        'output_format': 'tuples_list'
    }

    conf_yaml_fn = os.path.join(
        PYTEST_CALLING_DIR,
        NETCDF_HARVESTER_CONFIG__VALID
    )

    with open(conf_yaml_fn, 'w', encoding='utf8') as file:
        documents = yaml.dump(harvester_control_dict, file)
        print(f'conf_dict: {conf_yaml_fn}, documents: {documents}')

    hc = HarvestInnovStatsRequest(harvester_control_dict)
    hc.submit()
