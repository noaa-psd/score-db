"""
Copyright 2022 NOAA
All rights reserved.

Collection request handlers registrations.  This module helps define
the request handler format as well as the module definitions for each
request type

"""
from collections import namedtuple
from regions import RegionRequest
from experiments import ExperimentRequest
from db_action_response import DbActionResponse
from expt_metrics import ExptMetricRequest
from metric_types import MetricTypeRequest
from harvest_innov_stats import HarvestInnovStatsRequest
from plot_innov_stats import PlotInnovStatsRequest


NAMED_TUPLES_LIST = 'tuples_list'
PANDAS_DATAFRAME = 'pandas_dataframe'

INNOV_TEMPERATURE_NETCDF = 'innov_temperature_netcdf'


RequestHandler = namedtuple(
    'RequestHandler',
    [
        'description',
        'request',
        'result'
    ],
)

request_registry = {
    'region': RequestHandler(
        'Add or get regions',
        RegionRequest,
        DbActionResponse
    ),
    'experiment': RequestHandler(
        'Add or get or update experiment registration data',
        ExperimentRequest,
        DbActionResponse
    ),
    'expt_metrics': RequestHandler(
        'Add or get or update experiment metrics data',
        ExptMetricRequest,
        DbActionResponse
    ),
    'metric_types': RequestHandler(
        'Add or get or update metric types',
        MetricTypeRequest,
        DbActionResponse
    ),
    'harvest_innov_stats': RequestHandler(
        'Gather and store innovation statistics from diagnostics files',
        HarvestInnovStatsRequest,
        DbActionResponse
    ),
    'plot_innov_stats': RequestHandler(
        'plot innovation statistics',
        PlotInnovStatsRequest,
        DbActionResponse
    ),
}
