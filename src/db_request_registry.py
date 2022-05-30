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
    )
}
