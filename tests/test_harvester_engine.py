"""
Copyright 2022 NOAA
All rights reserved.

Unit tests for io_utils

"""
import os
import pathlib
import pytest
import json


from harvest_controller import HarvestController
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
EXP_NAME = 'UFSRNR_GSI_SOCA_3DVAR_COUPLED_122015_HC44RS_lstr_tst/'
GSI_3DVAR = '%Y%m%d%H%M%S/post/gsi/3dvar/'
SOCA_3DVAR = '%Y%m%d%H%M%S/post/soca/3dvar/'


def test_run_innov_stats_harvester_for_date_range():

    harvester_control_dict = {
        'date_range': {
            'datetime_str': '%Y-%m-%d %H:%M:%S',
            'start': '2015-12-01 00:00:00',
            'end': '2015-12-01 18:00:00'
        },
        'expt_name': 'UFSRNR_GSI_SOCA_3DVAR_COUPLED_122015_HC44RS_lstr_tst',
        'expt_wallclk_strt': '2022-08-03 02:40:34',
        'files': [
            {
                'filepath': BASE + EXP_NAME + GSI_3DVAR,
                'filename': 'innov_stats.metric.%Y%m%d%H.nc',
                'cycles': CYCLES,
                'harvester': 'innov_temperature_netcdf',
                'metrics': ['temperature','spechumid','uvwind'],
                'stats': ['bias', 'count', 'rmsd'],
                'elevation_unit': 'plevs'
            },
            {
                'filepath': BASE + EXP_NAME + SOCA_3DVAR,
                'filename': 'innov_stats.metric.%Y%m%d%H%M%S.nc',
                'cycles': [CYCLES[1]],
                'harvester': 'innov_temperature_netcdf',
                'metrics': ['temperature','salinity'],
                'stats': ['bias', 'rmsd'],
                'elevation_unit': 'depth'
            },
        ],
        'output_format': 'tuples_list'
    }

    hc = HarvestController(harvester_control_dict)
    hc.harvest_data()
