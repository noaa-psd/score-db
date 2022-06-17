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
from db_action_response import DbActionResponse
import score_table_models as stm
from score_table_models import MetricType as mt

from pandas import DataFrame
import sqlalchemy as db
from sqlalchemy.dialects.postgresql import insert
from sqlalchemy.inspection import inspect
from sqlalchemy import and_, or_, not_
from sqlalchemy import asc, desc
from sqlalchemy.sql import func


INSERT = 'INSERT'
UPDATE = 'UPDATE'

EXACT_DATETIME = 'exact'
FROM_DATETIME = 'from'
TO_DATETIME = 'to'
EXAMPLE_TIME = datetime(2022, 1, 14, 6, 23, 41)

ASCENDING = 'asc'
DESCENDING = 'desc'

VALID_ORDER_BY = [ASCENDING, DESCENDING]

HTTP_GET = 'GET'
HTTP_PUT = 'PUT'

VALID_METHODS = [HTTP_GET, HTTP_PUT]

DEFAULT_DATETIME_FORMAT_STR = '%Y-%m-%d_%H:%M:%S'


MetricTypeData = namedtuple(
    'MetricTypeData',
    [
        'name',
        'measurement_type',
        'measurement_units',
        'stat_type',
        'description'
    ],
)


def validate_method(method):
    if method not in VALID_METHODS:
        msg = f'Request type must be one of: {VALID_METHODS}, actually: ' \
            f'{method}'
        print(msg)
        raise ValueError(msg)
    
    return method


@dataclass
class MetricType:
    ''' metric type object storing data related to the measurement type '''
    name: str
    measurement_type: str
    measurement_units: str
    stat_type: str
    description: dict
    metric_type_data: MetricTypeData = field(init=False)

    def __post_init__(self):
        print(f'in post init name: {self.name}')
        print(f'description: {self.description}')
        self.metric_type_data = MetricTypeData(
            self.name,
            self.measurement_type,
            self.measurement_units,
            self.stat_type,
            self.description
        )


    def __repr__(self):
        return f'metric_type_data: {self.metric_type_data}'


    def get_metric_type_data(self):
        return self.metric_type_data


def get_metric_type_from_body(body):
    if not isinstance(body, dict):
        msg = 'The \'body\' key must be a type dict, actually ' \
            f'{type(body)}'
        raise TypeError(msg)
    
    try:
        description = json.loads(body.get('description'))
    except Exception as err:
        msg = 'Error loading \'description\', must be valid JSON - err: {err}'
        raise ValueError(msg) from err

    metric_type = MetricType(
        body.get('name'),
        body.get('measurement_type'),
        body.get('measurement_units'),
        body.get('stat_type'),
        description
    )
    
    return metric_type


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


def get_string_filter(filters, cls, key, constructed_filter):
    if not isinstance(filters, dict):
        msg = f'Invalid type for filters, must be \'dict\', actually ' \
            f'type: {type(filters)}'
        raise TypeError(msg)

    print(f'Column \'{key}\' is of type {type(getattr(cls, key).type)}.')
    string_flt = filters.get(key)

    if string_flt is None:
        print(f'No \'{key}\' filter detected')
        return constructed_filter

    like_filter = string_flt.get('like')
    # prefer like search over exact match if both exist
    if like_filter is not None:
        constructed_filter[key] = (getattr(cls, key).like(like_filter))
        return constructed_filter

    exact_match_filter = string_flt.get('exact')
    if exact_match_filter is not None:
        constructed_filter[key] = (getattr(cls, key) == exact_match_filter)

    return constructed_filter


def construct_filters(filters):
    constructed_filter = {}

    constructed_filter = get_string_filter(
        filters, mt, 'name', constructed_filter)

    constructed_filter = get_string_filter(
        filters, mt, 'measurement_type', constructed_filter)

    constructed_filter = get_string_filter(
        filters, mt, 'measurement_units', constructed_filter)

    constructed_filter = get_string_filter(
        filters, mt, 'stat_type', constructed_filter)
    
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


def get_all_metric_types():
    request_dict = {
        'name': 'metric_type',
        'method': 'GET'
    }

    mtr = MetricTypeRequest(request_dict)
    return mtr.submit()


@dataclass
class MetricTypeRequest:
    request_dict: dict
    method: str = field(default_factory=str, init=False)
    params: dict = field(default_factory=dict, init=False)
    filters: dict = field(default_factory=dict, init=False)
    ordering: list = field(default_factory=list, init=False)
    record_limit: int = field(default_factory=int, init=False)
    body: dict = field(default_factory=dict, init=False)
    metric_type: MetricType = field(init=False)
    metric_type_data: namedtuple = field(init=False)
    response: dict = field(default_factory=dict, init=False)


    def __post_init__(self):
        method = self.request_dict.get('method')
        self.method = validate_method(self.request_dict.get('method'))
        self.params = self.request_dict.get('params')

        self.body = self.request_dict.get('body')
        if self.method == HTTP_PUT:
            self.metric_type = get_metric_type_from_body(self.body)
            # pprint(f'metric_type : {repr(self.metric_type)}')
            self.metric_type_data = self.metric_type.get_metric_type_data()
            for k, v in zip(
                self.metric_type_data._fields, self.metric_type_data
            ):
                val = pprint.pformat(v, indent=4)
                print(f'exp_data: k: {k}, v: {val}')
        else:
            print(f'In MetricTypeRequest - params: {self.params}')
            if isinstance(self.params, dict):
                self.filters = construct_filters(self.params.get('filters'))
                self.ordering = self.params.get('ordering')
                self.record_limit = self.params.get('record_limit')
            
                if not type(self.record_limit) == int or self.record_limit <= 0:
                    self.record_limit = None
            else:
                self.filters = None
                self.ordering = None
                self.record_limit = None


    def failed_request(self, error_msg):
        return DbActionResponse(
            request=self.request_dict,
            success=False,
            message='Failed metric type request.',
            details=None,
            errors=error_msg
        )


    def submit(self):

        if self.method == HTTP_GET:
            return self.get_metric_types()
        elif self.method == HTTP_PUT:
            # becomes an update if record exists
            try:
                return self.put_metric_type()
            except Exception as err:
                error_msg = 'Failed to insert metric type record -' \
                    f' err: {err}'
                print(f'Submit PUT error: {error_msg}')
                return self.failed_request(error_msg)

    
    def put_metric_type(self):
        engine = stm.get_engine_from_settings()
        session = stm.get_session()

        insert_stmt = insert(mt).values(
            name=self.metric_type_data.name,
            measurement_type=self.metric_type_data.measurement_type,
            measurement_units=self.metric_type_data.measurement_units,
            stat_type=self.metric_type_data.stat_type,
            description=self.metric_type_data.description,
            created_at=datetime.utcnow(),
            updated_at=None
        ).returning(mt)
        print(f'insert_stmt: {insert_stmt}')

        time_now = datetime.utcnow()

        do_update_stmt = insert_stmt.on_conflict_do_update(
            constraint='unique_metric_type',
            set_=dict(
                # group_id=self.experiment_data.group_id,
                description=self.metric_type_data.description,
                updated_at=time_now
            )
        )

        print(f'do_update_stmt: {do_update_stmt}')

        try:
            result = session.execute(do_update_stmt)
            session.flush()
            result_row = result.fetchone()
            action = INSERT
            if result_row.updated_at is not None:
                action = UPDATE
            # print(f'result.fetchone(): {result_row}')
            # print(f'updated_at: {result_row.updated_at}')
            # print(f'result.fetchone().keys(): {result_row._mapping}')

            session.commit()
            session.close()
        except Exception as err:
            action = INSERT
            message = f'Attempt to {action} metric type record FAILED'
            error_msg = f'Failed to insert/update record - err: {err}'
            print(f'error_msg: {error_msg}')
        else:
            message = f'Attempt to {action} experiment record SUCCEEDED'
            error_msg = None
        
        results = {}
        if result_row is not None:
            results['action'] = action
            results['data'] = [result_row._mapping]
            results['id'] = result_row.id

        response = DbActionResponse(
            self.request_dict,
            (error_msg is None),
            message,
            results,
            error_msg
        )

        print(f'response: {response}')
        return response

    
    def get_metric_types(self):
        engine = stm.get_engine_from_settings()
        session = stm.get_session()

        q = session.query(
            mt.id,
            mt.name,
            mt.measurement_type,
            mt.measurement_units,
            mt.stat_type,
            mt.description,
            mt.created_at,
            mt.updated_at
        ).select_from(
            mt
        )

        print('Before adding filters to metric types request########################')
        if self.filters is not None and len(self.filters) > 0:
            for key, value in self.filters.items():
                q = q.filter(value)
        
        print('After adding filters to metric types request########################')
        
        # add column ordering
        column_ordering = build_column_ordering(mt, self.ordering)
        if column_ordering is not None and len(column_ordering) > 0:
            for ordering_item in column_ordering:
                q = q.order_by(ordering_item)

        # limit number of returned records
        if self.record_limit is not None and self.record_limit > 0:
            q = q.limit(self.record_limit)

        metric_types = q.all()

        results = DataFrame()
        error_msg = None
        record_count = 0
        try:
            if len(metric_types) > 0:
                results = DataFrame(metric_types, columns = metric_types[0]._fields)
            
        except Exception as err:
            message = 'Request for metric type records FAILED'
            error_msg = f'Failed to get metric type  records - err: {err}'
        else:
            message = 'Request for metric type records SUCCEEDED'
            for idx, row in results.iterrows():
                print(f'idx: {idx}, row: {row}')
            record_count = len(results.index)
        
        details = {}
        # details['filters'] = self.filters
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
