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


from plot_innov_stats import PlotInnovStatsRequest
from score_table_models import ExperimentMetric as exp_metrics_table
import expt_metrics
from expt_metrics import ExptMetricInputData, ExptMetric, ExptMetricRequest

PYTEST_CALLING_DIR = pathlib.Path(__file__).parent.resolve()
PLOT_INNOV_STATS_CONFIG__VALID = 'plot_innov_stats_config__valid.yaml'
FIGURES_DIR = 'figures'

figures_basepath = os.path.join(
    PYTEST_CALLING_DIR,
    FIGURES_DIR
)

CYCLES = [0, 21600, 43200, 64800]


def test_plot_innov_stats_for_date_range():

    plot_control_dict = {
        'db_request_name': 'plot_innov_stats',
        'date_range': {
            'datetime_str': '%Y-%m-%d %H:%M:%S',
            'start': '2016-01-01 00:00:00',
            'end': '2016-01-31 18:00:00'
        },
        'experiments': [
            {
                'name': 'C96L64.UFSRNR.GSI_3DVAR.012016',
                'wallclock_start': '2021-07-22 09:22:05',
                'graph_label': 'C96L64 GSI Uncoupled 3DVAR Experiment',
                'graph_color': 'blue'
            },
            {
                'name': 'C96L64.UFSRNR.GSI_SOCA_3DVAR.012016',
                'wallclock_start':  '2021-07-24 11:31:16',
                'graph_label': 'C96L64 GSI and SOCA Coupled 3DVAR Experiment',
                'graph_color': 'red'
            }
        ],
        'stat_groups': [
            {
                'cycles': CYCLES,
                'stat_group_frmt_str': 'innov_stats_{metric}_{stat}',
                'metrics': ['temperature','spechumid','uvwind'],
                'stats': ['bias', 'rmsd'],
                'elevation_unit': 'plevs',
                'regions': [
                    'equatorial',
                    'global',
                    'north_hemis',
                    'south_hemis',
                    'tropics'
                ]
            },
            # {
            #     'cycles': [CYCLES[1]],
            #     'stat_group_frmt_str': 'innov_stats_{metric}_{stat}',
            #     'metrics': ['temperature','salinity'],
            #     'stats': ['bias', 'rmsd'],
            #     'elevation_unit': 'depth',
            #     'regions': [
            #         'equatorial',
            #         'global',
            #         'north_hemis',
            #         'south_hemis',
            #         'tropics'
            #     ]
            # },
        ],
        'work_dir': figures_basepath,
        'fig_base_fn': 'C96L64_GSI_3DVAR_VS_GSI_SOCA_3DVAR'
    }
    
    conf_yaml_fn = os.path.join(
        PYTEST_CALLING_DIR,
        PLOT_INNOV_STATS_CONFIG__VALID
    )

    with open(conf_yaml_fn, 'w', encoding='utf8') as file:
        documents = yaml.dump(plot_control_dict, file)
        print(f'conf_dict: {conf_yaml_fn}, documents: {documents}')

    # plot_request = PlotInnovStatsRequest(plot_control_dict)
    # plot_request.submit()