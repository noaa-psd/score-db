"""
Copyright 2022 NOAA
All rights reserved.

Collection of methods to facilitate handling of score db requests

"""

from collections import namedtuple
import copy
from dataclasses import dataclass, field
from datetime import datetime
import json
import pprint
import traceback
from db_action_response import DbActionResponse
import score_table_models as stm
from score_table_models import Experiment as exp
from score_table_models import ExperimentMetric as ex_mt
from experiments import Experiment, ExperimentData
from experiments import ExperimentRequest
import regions as rgs
import metric_types as mt
import numpy as np
from psycopg2.extensions import register_adapter, AsIs
import psycopg2


from pandas import DataFrame
import sqlalchemy as db
from sqlalchemy.dialects.postgresql import insert
from sqlalchemy.inspection import inspect
from sqlalchemy import and_, or_, not_
from sqlalchemy import asc, desc
from sqlalchemy.sql import func

psycopg2.extensions.register_adapter(np.int64, psycopg2._psycopg.AsIs)

INSERT = 'INSERT'
UPDATE = 'UPDATE'

FROM_DATETIME = 'from'
TO_DATETIME = 'to'
EXACT_DATETIME = 'exact'
EXAMPLE_TIME = datetime(2022, 1, 14, 6, 23, 41)

ASCENDING = 'asc'
DESCENDING = 'desc'

VALID_ORDER_BY = [ASCENDING, DESCENDING]

HTTP_GET = 'GET'
HTTP_PUT = 'PUT'

VALID_METHODS = [HTTP_GET, HTTP_PUT]

DEFAULT_DATETIME_FORMAT_STR = '%Y-%m-%d_%H:%M:%S'


ExptMetricInputData = namedtuple(
    'ExptMetricInputData',
    [
        'name',
        'region_name',
        'elevation',
        'elevation_unit',
        'value',
        'time_valid'
    ],
)


ExptMetricsData = namedtuple(
    'ExptMetricsData',
    [
        'id',
        'name',
        'elevation',
        'elevation_unit',
        'value',
        'time_valid',
        'expt_name',
        'wallclock_start',
        'metric_type',
        'metric_unit',
        'metric_stat_type',
        'region',
        'created_at'
    ],
)


class ExptMetricsError(Exception):
    def __init__(self, m):
        self.message = m
    def __str__(self):
        return self.message


def validate_method(method):
    if method not in VALID_METHODS:
        msg = f'Request type must be one of: {VALID_METHODS}, actually: {method}'
        print(msg)
        raise ValueError(msg)
    
    return method


@dataclass
class ExptMetric:
    ''' region object storing region name and min/max latitude bounds '''
    name: str
    cycle_start: datetime
    cycle_stop: datetime
    owner_id: str
    group_id: str
    experiment_type: str
    platform: str
    wallclock_start: datetime
    wallclock_end: datetime
    description: dict
    experiment_data: ExperimentData = field(init=False)

    def __post_init__(self):
        print(f'in post init name: {self.name}')
        if self.cycle_start > self.cycle_stop:
            msg = f'start time must be before end time - start: {self.cycle_start}, ' \
                f'end: {self.cycle_stop}'
            raise ValueError(msg)
        if self.platform not in VALID_PLATFORMS:
            msg = f'\'platform\' must be one of {VALID_PLATFORMS}, was ' \
                f'\'{self.platform}\''
            raise ValueError(msg)
        
        print(f'description: {self.description}')
        self.experiment_data = ExperimentData(
            self.name,
            self.cycle_start,
            self.cycle_stop,
            self.owner_id,
            self.group_id,
            self.experiment_type,
            self.platform,
            self.wallclock_start,
            self.wallclock_end,
            self.description
        )


    def __repr__(self):
        return f'experiment_data: {self.experiment_data}'


    def get_experiment_data(self):
        return self.experiment_data


def get_formatted_time(value, format_str):
    try:
        time_str = datetime.strftime(value, format_str)
    except Exception as err:
        msg = f'Problem formatting time: {value} with format_str' \
            f' \'{format_str}\'. error: {err}.'
        raise ValueError(msg) from err


def get_time(value, datetime_format=None):
    if value is None:
        return None

    if datetime_format is None:
        datetime_format = DEFAULT_DATETIME_FORMAT_STR
    
    example_time = get_formatted_time(EXAMPLE_TIME, datetime_format)
        
    try:
        parsed_time = datetime.strptime(
            value, DEFAULT_DATETIME_FORMAT_STR
        )
    except Exception as err:
        msg = 'Invalid datetime format, must be ' \
            f'of \'{datetime_format}\'. ' \
            f'For example: \'{example_time}\'.' \
            f' Error: {err}.'
        raise ValueError(msg) from err
    
    return parsed_time


def get_time_filter(filter_dict, cls, key, constructed_filter):
    if not isinstance(filter_dict, dict):
        msg = f'Invalid type for filters, must be \'dict\', actually ' \
            f'type: {type(filter_dict)}'
        raise TypeError(msg)

    value = filter_dict.get(key)
    if value is None:
        print(f'No \'{key}\' filter detected')
        return constructed_filter

    exact_datetime = get_time(value.get(EXACT_DATETIME))
    print(f'exact_datetime: {exact_datetime}')
    if exact_datetime is not None:
        constructed_filter[key] = (
            getattr(cls, key) == exact_datetime
        )
        return constructed_filter

    from_datetime = get_time(value.get(FROM_DATETIME))
    to_datetime = get_time(value.get(TO_DATETIME))
    
    
    print(f'Column \'{key}\' is of type {type(getattr(cls, key).type)}.')

    if from_datetime is not None and to_datetime is not None:
        if to_datetime < from_datetime:
            raise ValueError('\'from\' must be older than \'to\'')
        
        constructed_filter[key] = and_(
            getattr(cls, key) >= from_datetime,
            getattr(cls, key) <= to_datetime
        )
    elif from_datetime is not None:
        constructed_filter[key] = (
            getattr(cls, key) >= from_datetime
        )
    elif to_datetime is not None:
        constructed_filter[key] = (
            getattr(cls, key) <= to_datetime
        )

    print(f'constructed_filter: {constructed_filter}')
    return constructed_filter


def validate_list_of_strings(values):
    if isinstance(values, str):
        val_list = []
        val_list.append(values)
        return val_list

    if not isinstance(values, list):
        msg = f'string values must be a list - actually: {type(values)}'
        raise TypeError(msg)
    
    for value in values:
        if not isinstance(value, str):
            msg = 'all values must be string type - value: ' \
                f'{value} is type: {type(value)}'
            raise TypeError(msg)


def get_string_filter(filter_dict, cls, key, constructed_filter):
    if not isinstance(filter_dict, dict):
        msg = f'Invalid type for filters, must be \'dict\', actually ' \
            f'type: {type(filter_dict)}'
        raise TypeError(msg)

    print(f'Column \'{key}\' is of type {type(getattr(cls, key).type)}.')
    string_flt = filter_dict.get(key)
    print(f'string_flt: {string_flt}')

    if string_flt is None:
        print(f'No \'{key}\' filter detected')
        return constructed_filter

    like_filter = string_flt.get('like')
    # prefer like search over exact match if both exist
    if like_filter is not None:
        constructed_filter[key] = (getattr(cls, key).like(like_filter))
        return constructed_filter

    exact_match_filter = validate_list_of_strings(string_flt.get('exact'))
    if exact_match_filter is not None:
        constructed_filter[key] = (getattr(cls, key).in_(exact_match_filter))

    return constructed_filter


def get_experiments_filter(filter_dict, constructed_filter):
    if not isinstance(filter_dict, dict):
        msg = f'Invalid type for filter, must be \'dict\', actually ' \
            f'type: {type(filter_dict)}'
        raise TypeError(msg)
    
    if not isinstance(constructed_filter, dict):
        msg = 'Invalid type for constructed_filter, must be \'dict\', ' \
            f'actually type: {type(filter_dict)}'
        raise TypeError(msg)   
    
    constructed_filter = get_string_filter(
        filter_dict, exp, 'name', constructed_filter)
    
    constructed_filter = get_time_filter(
        filter_dict, exp, 'cycle_start', constructed_filter)

    constructed_filter = get_time_filter(
        filter_dict, exp, 'cycle_stop', constructed_filter)

    constructed_filter = get_time_filter(
        filter_dict, exp, 'wallclock_start', constructed_filter)
    
    return constructed_filter


def get_metric_types_filter(filter_dict, constructed_filter):
    if filter_dict is None:
        return constructed_filter

    if not isinstance(filter_dict, dict):
        msg = f'Invalid type for filter, must be \'dict\', actually ' \
            f'type: {type(filter_dict)}'
        raise TypeError(msg)
    
    if not isinstance(constructed_filter, dict):
        msg = 'Invalid type for constructed_filter, must be \'dict\', ' \
            f'actually type: {type(filter_dict)}'
        raise TypeError(msg)   
    
    constructed_filter = get_string_filter(
        filter_dict, mt, 'name', constructed_filter)
    
    constructed_filter = get_string_filter(
        filter_dict, mt, 'measurement_type', constructed_filter)
    
    constructed_filter = get_string_filter(
        filter_dict, mt, 'measurement_unit', constructed_filter)
    
    constructed_filter = get_string_filter(
        filter_dict, mt, 'stat_type', stat_type)

    return constructed_filter


def get_regions_filter(filter_dict, constructed_filter):
    if filter_dict is None:
        return constructed_filter

    if not isinstance(filter_dict, dict):
        msg = f'Invalid type for filter, must be \'dict\', actually ' \
            f'type: {type(filter_dict)}'
        raise TypeError(msg)
    
    if not isinstance(constructed_filter, dict):
        msg = 'Invalid type for constructed_filter, must be \'dict\', ' \
            f'actually type: {type(filter_dict)}'
        raise TypeError(msg)   
    
    constructed_filter = get_string_filter(
        filter_dict, rgs, 'name', constructed_filter)

    return constructed_filter


def validate_column_name(cls, value):
    if not isinstance(value, str):
        raise TypeError(f'Column name must be a str, was {type(value)}')

    try:
        column_obj = getattr(cls, value)
        print(f'column: {column_obj}, type(key): {type(column_obj)}')
    except Exception as err:
        msg = f'Column does not exist - err: {err}'
        raise ValueError(msg)
    
    return column_obj


def validate_order_dir(value):
    if not isinstance(value, str):
        raise TypeError(f'\'order_by\' must be a str, was {type(value)}')
    
    if value not in VALID_ORDER_BY:
        raise TypeError(f'\'order_by\' must be one of {VALID_ORDER_BY}, ' \
            f' was {value}')
    
    return value


def build_column_ordering(cls, ordering):
    if ordering is None:
        return None
    
    if not isinstance(ordering, list):
        msg = f'\'order_by\' must be a list - type(ordering): ' \
            f'{type(ordering)}'
        raise TypeError(msg)

    constructed_ordering = []
    for value in ordering:
        print(f'value: {value}')

        if type(value) != dict:
            msg = f'List items must be a type-dict - was {type(value)}'
            raise TypeError(msg)

        col_obj = validate_column_name(cls, value.get('name'))
        order_by = validate_order_dir(value.get('order_by'))

        if order_by == ASCENDING:
            constructed_ordering.append(asc(col_obj))
        else:
            constructed_ordering.append(desc(col_obj))
    
    print(f'constructed_ordering: {constructed_ordering}')
    return constructed_ordering


def get_expt_record(body):

    # get experiment name
    engine = stm.get_engine_from_settings()
    session = stm.get_session()
    expt_name = body.get('expt_name')
    datestr_format = body.get('datestr_format')
    wlclk_strt_str = body.get('expt_wallclock_start')
    expt_wallclock_start = get_time(
        wlclk_strt_str,
        datestr_format
    )

    expt_request = {
        'name': 'experiment',
        'method': 'GET',
        'params': {
            'filters': {
                'name': {
                    'exact': expt_name
                },
                'wallclock_start': {
                    'exact': wlclk_strt_str
                },
            },
            'ordering': [
                {'name': 'wallclock_start', 'order_by': 'desc'}
            ],
            'record_limit': 1
        }
    }

    er = ExperimentRequest(expt_request)
    results = er.submit()
    record_cnt = 0
    try:
        if results.success is True:
            records = results.details.get('records')
            record_cnt = records.shape[0]
        else:
            msg = f'Problems encountered requesting experiment data.'
            # create error return db_action_response
            raise ExptMetricsError(msg)
        if record_cnt <= 0:
            msg = 'Request for experiment record did not return a record'
            raise ExptMetricsError(msg)
        
    except Exception as err:
        msg = f'Problems encountered requesting experiment data. err - {err}'
        raise ExptMetricsError(msg)
        
    print(f'records: {records}, len(records): {records.shape[0]}')

    return records


@dataclass
class ExptMetricRequest:
    request_dict: dict
    method: str = field(default_factory=str, init=False)
    params: dict = field(default_factory=dict, init=False)
    filters: dict = field(default_factory=dict, init=False)
    ordering: list = field(default_factory=dict, init=False)
    record_limit: int = field(default_factory=dict, init=False)
    body: dict = field(default_factory=dict, init=False)
    experiment: Experiment = field(init=False)
    expt_id: int = field(default_factory=int, init=False)
    response: dict = field(default_factory=dict, init=False)

    def __post_init__(self):
        method = self.request_dict.get('method')
        self.method = validate_method(self.request_dict.get('method'))
        self.params = self.request_dict.get('params')
        self.body = self.request_dict.get('body')
        self.filters = None
        self.ordering = None
        self.record_limit = None
        if self.params is not None:
            self.filters = self.params.get('filters')
            self.ordering = self.params.get('ordering')
            self.record_limit = self.params.get('record_limit')
        
        print(f'self.params: {self.params}')
        print(f'self.filters: {self.filters}')

    def submit(self):

        if self.method == HTTP_GET:
            try:
                return self.get_experiment_metrics()
            except Exception as err:
                trcbk = traceback.format_exc()
                error_msg = 'Failed to get experiment metric records -' \
                    f' trcbk: {trcbk}'
                print(f'Submit GET error: {error_msg}')
                return self.failed_request(error_msg)
        elif self.method == HTTP_PUT:
            # becomes an update if record exists
            try:
                return self.put_expt_metrics_data()
            except Exception as err:
                trcbk = traceback.format_exc()
                error_msg = 'Failed to insert experiment metric records -' \
                    f' trcbk: {trcbk}'
                print(f'Submit PUT error: {error_msg}')
                return self.failed_request(error_msg)


    def failed_request(self, error_msg):
        return DbActionResponse(
            request=self.request_dict,
            success=False,
            message='Failed experiment metrics request.',
            details=None,
            errors=error_msg
        )


    def construct_filters(self, query):
        if not isinstance(self.filters, dict):
            msg = f'Filters must be of the form dict, filters: {type(self.filters)}'
            raise ExptMetricsError(msg)

        constructed_filter = {}

        # filter experiment metrics table for all matching experiments
        constructed_filter = get_experiments_filter(
            self.filters.get('experiments'), constructed_filter)

        # get only those records related to certain experiment

        constructed_filter = get_metric_types_filter(
            self.filters.get('metric_types'), constructed_filter)
        
        constructed_filter = get_regions_filter(
            self.filters.get('regions'), constructed_filter)

        constructed_filter = get_time_filter(
            self.filters, ex_mt, 'time_valid', constructed_filter)

        if len(constructed_filter) > 0:
            print(f'constructed_filters: {constructed_filter}')
            try:
                for key, value in constructed_filter.items():
                    query = query.filter(value)
            except Exception as err:
                msg = f'Problems adding filter to query - query: {query}, ' \
                    f'filter: {value}, err: {err}'
                raise ExptMetricsError(error_msg) from err

        return query


    def get_first_expt_id_from_df(self, record):
        try:
            self.expt_id = record[exp.id.name].iat[0]
        except Exception as err:
            error_msg = f'Problem finding experiment id from record: {record} ' \
                f'- err: {err}'
            print(f'error_msg: {error_msg}')
            raise ExptMetricsError(error_msg) 
        return self.expt_id
    

    def parse_metrics_data(self, metrics):
        if not isinstance(metrics, list):
            msg = f'\'metrics\' must be a list - was a \'{type(metrics)}\''
            raise ExptMetricsError(msg)
        
        unique_regions = set()
        unique_metric_types = set()
        for metric in metrics:

            print(f'metric: {metric}')
            if not isinstance(metric, ExptMetricInputData):
                msg = 'Each metric must be a type ' \
                    f'\'{type(ExptMetricInputData)}\' was \'{metric}\''
                print(f'metric: {metric}, msg: {msg}')
                raise ExptMetricsError(msg)
            
            unique_regions.add(metric.region_name)
            unique_metric_types.add(metric.name)

        regions = rgs.get_regions_from_name_list(list(unique_regions))
        print(f'regions.details: {regions.details}')
        print(f'unique_metric_types: {unique_metric_types}')
        metric_types = mt.get_all_metric_types()
        print(f'metric_types.details: {metric_types.details}')

        rg_df = regions.details.get('records')
        if rg_df.shape[0] != len(unique_regions):
            msg = 'Did not find all unique_regions in regions table ' \
                f'unique_regions: {len(unique_regions)}, found regions: ' \
                f'{rg_df.shape[0]}.'
            print(f'region counts do not match: {msg}')
            raise ExptMetricsError(msg)

        print(f'regions: {rg_df}')

        mt_df = metric_types.details.get('records')
        print(f'metric_types_df: {mt_df}')
        # metrics_df = DataFrame(metrics, columns=ExptMetricInputData._fields)
        records = []
        for row in metrics:
            print(f'row.name: {row.name}')
            metric_type_id = mt_df.loc[mt_df['name'] == row.name, 'id'].item()
            region_id = rg_df.loc[
                rg_df['name'] == row.region_name, 'id'
            ].item()
            print(f'metric_type_id: {metric_type_id}, region_id: {region_id}')

            item = ex_mt(
                experiment_id=self.expt_id,
                metric_type_id=metric_type_id,
                region_id=region_id,
                elevation=row.elevation,
                elevation_unit=row.elevation_unit,
                value=row.value,
                time_valid=row.time_valid
            )

            print(f'item: {item}')
            records.append(item)
        
        return records


    def get_expt_metrics_from_body(self, body):
        if not isinstance(body, dict):
            error_msg = 'The \'body\' key must be a type dict, actually ' \
                f'{type(body)}'
            print(f'Metrics key not found: {error_msg}')
            raise ExptMetricsError(error_msg)
        
        # parse experiment name from body
        # parse method type from body
        # parse experiment metric data from body and transform into pandas df
        # get date format from body

        metrics = body.get('metrics')
        parsed_metrics = self.parse_metrics_data(metrics)

        
        
        return parsed_metrics

    
    def put_expt_metrics_data(self):

        # we need to determine the primary key id from the experiment
        # all calls to this function must return a DbActionResponse object
        expt_record = get_expt_record(self.body)
        print(f'expt_record: {expt_record}')
        expt_id = self.get_first_expt_id_from_df(expt_record)
        print(f'In put_expt_metrics_data: expt_id: {self.expt_id}')
        records = self.get_expt_metrics_from_body(self.body)

        engine = stm.get_engine_from_settings()
        session = stm.get_session()

        try:
            if len(records) > 0:
                print(f'records: {records}')
                print(f'ex_mt: {ex_mt}')
                session.bulk_save_objects(records)
                session.commit()
                session.close()
                # insert_stmt = insert(ex_mt).values(records).returning(ex_mt.id)
                # print(f'insert_stmt: {insert_stmt}')
                # ids = session.execute(insert_stmt).fetchall()
                # print(f'inserted ids: {ids}')

        except Exception as err:
            print(f'Failed to insert records: {err}')

    
    def get_experiment_metrics(self):
        engine = stm.get_engine_from_settings()
        session = stm.get_session()

        # set basic query
        q = session.query(ex_mt)

        # add filters
        q = self.construct_filters(q)

        # # add column ordering
        column_ordering = build_column_ordering(ex_mt, self.ordering)
        if column_ordering is not None and len(column_ordering) > 0:
            for ordering_item in column_ordering:
                q = q.order_by(ordering_item)

        metrics = q.all()
        print(f'metrics: {metrics}')
        parsed_metrics = []
        for metric in metrics:
            record = ExptMetricsData(
                id=metric.id,
                name=metric.metric_type.name,
                elevation=metric.elevation,
                elevation_unit=metric.elevation_unit,
                value=metric.value,
                time_valid=metric.time_valid,
                expt_name=metric.experiment.name,
                wallclock_start=metric.experiment.wallclock_start,
                metric_type=metric.metric_type.measurement_type,
                metric_unit=metric.metric_type.measurement_units,
                metric_stat_type=metric.metric_type.stat_type,
                region=metric.region.name,
                created_at=metric.created_at
            )
            parsed_metrics.append(record)
            print(f'metric.metric_type: {metric.metric_type.name}')
            print(f'experiment: {metric.experiment.name}')
            print(f'metric: {metric.__dict__}')
        



        # # limit number of returned records
        # if self.record_limit is not None:
        #     q = q.limit(self.record_limit)

        # experiments = q.all()

        results = DataFrame()
        error_msg = None
        record_count = 0
        try:
            if len(parsed_metrics) > 0:
                results = DataFrame(
                    parsed_metrics,
                    columns = parsed_metrics[0]._fields
                )
            
        except Exception as err:
            message = 'Request for experiment metric records FAILED'
            trcbk = traceback.format_exc()
            error_msg = f'Failed to get any experiment metrics - err: {trcbk}'
            print(f'error_msg: {error_msg}')
        else:
            message = 'Request for experiment metrics SUCCEEDED'
            for idx, row in results.iterrows():
                print(f'idx: {idx}, row: {row}')
            record_count = len(results.index)
        
        details = {}
        details['record_count'] = record_count

        if record_count > 0:
            details['records'] = results

        response = DbActionResponse(
            self.request_dict,
            (error_msg is None),
            message,
            details,
            error_msg
        )

        print(f'response: {response}')

        return response
