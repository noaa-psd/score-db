"""
Copyright 2022 NOAA
All rights reserved.

Collection of methods to facilitate insertion/selection of experiment
records.  Each experiment record contains the following columns
['id', 'cycle_start', cycle_stop', 'owner_id', 'group_id', 'experiment_type'
'platform', 'wallclock_start', 'wallclock_end', 'description', 'created_at'
'updated_at'].  The 'description' column is unstructured JSON and is meant
to store the experiment configuration.  Using sqlalchemy, keys within the
JSON column could be searched (although indexing should be addressed
if the experiment table grows significantly).

"""

from collections import namedtuple
import copy
from dataclasses import dataclass, field
from datetime import datetime
import json
import pprint
from db_action_response import DbActionResponse
import score_table_models as stm
from score_table_models import Experiment as exp

from pandas import DataFrame
import sqlalchemy as db
from sqlalchemy.dialects.postgresql import insert
from sqlalchemy.inspection import inspect
from sqlalchemy import and_, or_, not_
from sqlalchemy import asc, desc
from sqlalchemy.sql import func


HERA = 'hera'
ORION = 'orion'
PW_AZV1 = 'pw_azv1'
PW_AZV2 = 'pw_azv2'
PW_AWV1 = 'pw_awv1'
PW_AWV2 = 'pw_awv2'

VALID_PLATFORMS = [HERA, ORION, PW_AZV1, PW_AZV2, PW_AWV1, PW_AWV2]

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

DEFAULT_DATETIME_FORMAT_STR = '%Y-%m-%d %H:%M:%S'

ExperimentData = namedtuple(
    'ExperimentData',
    [
        'name',
        'cycle_start',
        'cycle_stop',
        'owner_id',
        'group_id',
        'experiment_type',
        'platform',
        'wallclock_start',
        'wallclock_end',
        'description'
    ],
)

def validate_method(method):
    if method not in VALID_METHODS:
        msg = f'Request type must be one of: {VALID_METHODS}, actually: {method}'
        print(msg)
        raise ValueError(msg)
    
    return method


@dataclass
class Experiment:
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



def get_experiment_from_body(body):
    if not isinstance(body, dict):
        msg = 'The \'body\' key must be a type dict, actually ' \
            f'{type(body)}'
        raise TypeError(msg)
    
    try:
        description = json.loads(body.get('description'))
    except Exception as err:
        msg = 'Error loading \'description\', must be valid JSON - err: {err}'
        raise ValueError(msg) from err

    datestr_format = body.get('datestr_format')
    CYCLE_START = 'cycle_start'
    CYCLE_STOP = 'cycle_stop'
    WALLCLOCK_START = 'wallclock_start'
    WALLCLOCK_END = 'wallclock_end'

    datetime_strs = [CYCLE_START, CYCLE_STOP, WALLCLOCK_START, WALLCLOCK_END]
    exp_dates = {}

    for date_var_str in datetime_strs:
        try:
            dstr = body.get(date_var_str)
            if dstr is None or dstr == 'None':
                exp_dates[date_var_str] = datetime(1970, 1, 1)
            else:
                exp_dates[date_var_str] = datetime.strptime(dstr, datestr_format)

        except Exception as err:
            msg = f'Problem parsing experiment \'{date_var_str}\': ' \
                f'{exp_dates[date_var_str]}, datestr_format: ' \
                f'{datestr_format}, err: {err}'
            raise ValueError(msg) from err

    experiment = Experiment(
        body.get('name'),
        exp_dates[CYCLE_START],
        exp_dates[CYCLE_STOP],
        body.get('owner_id'),
        body.get('group_id'),
        body.get('experiment_type'),
        body.get('platform'),
        exp_dates[WALLCLOCK_START],
        exp_dates[WALLCLOCK_END],
        description
    )
    
    return experiment

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


def get_time_filter(filter, cls, key, constructed_filter):
    """
        Build a datetime filter (otherwise known as a WHERE clause)

        Parameters:
        -----------
        filters: dict - this time filter, allows 'from' key and 'to' key where
            each are optional.  However, if neither 'from' or 'to' keys 
            are provided, this method will simly not add a filter.
            example dict: 
            {
                'from': '2015-01-01_00:00:00',
                'to': '2018-01-01_00:00:00'
            }
        cls: class object - this object can be any sqlalchemy table object
            such as Region, or Experiment
        key: str - this is a column name from the 'cls' table object.  The
            column defined by 'key' must be a DateTime type
        constructed_filter: dict - this is a dictionary containing all of the
            previously defined filter clauses.

        Returns: dict - returns the constructed_filter dict containing the
            newly created time filter pertaining to the table = 'cls' and
            the DateTime column 'key'.

        """
    if not isinstance(filter, dict):
        msg = f'Invalid type for filter, must be \'dict\', actually ' \
            f'type: {type(filters)}'
        raise TypeError(msg)

    bounds = filters.get(key)
    if bounds is None:
        print(f'No \'{key}\' filter detected')
        return constructed_filter

    exact_datetime = get_time(bounds.get(EXACT_DATETIME))
    print(f'exact_datetime: {exact_datetime}')
    if exact_datetime is not None:
        constructed_filter[key] = (
            getattr(cls, key) == exact_datetime
        )
        return constructed_filter

    from_datetime = get_time(bounds.get(FROM_DATETIME))
    to_datetime = get_time(bounds.get(TO_DATETIME))
    
    
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
    """
    Build a set of filters (otherwise known as a WHERE clause) pertaining
    to the experiments table.

    Parameters:
    -----------
    filters: dict - this filter dict contains all the column data filter
        information needed to build filter clauses.
        example dict: 
        {
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
            'group_id': {
                    'exact': 'gsienkf'
            },
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
            'wallclock_end': {
                'from': '2015-01-01_00:00:00',
                'to': '2022-05-01_00:00:00'
            }

        }

    """
    
    constructed_filter = {}

    constructed_filter = get_string_filter(
        filters, exp, 'name', constructed_filter)

    constructed_filter = get_time_filter(
        filters, exp, 'cycle_start', constructed_filter)

    constructed_filter = get_time_filter(
        filters, exp, 'cycle_stop', constructed_filter)

    constructed_filter = get_time_filter(
        filters, exp, 'wallclock_start', constructed_filter)

    constructed_filter = get_time_filter(
        filters, exp, 'wallclock_end', constructed_filter)

    constructed_filter = get_string_filter(
        filters, exp, 'owner_id', constructed_filter)

    constructed_filter = get_string_filter(
        filters, exp, 'group_id', constructed_filter)
    
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
    """
    Build a sequential list of column ordering (otherwise known as the 
    ORDER BY clause)
    pertaining
    to the experiments table.

    Parameters:
    -----------
    cls: class object - this object can be any sqlalchemy table object
            such as Region, or Experiment
 
    ordering: list - this is a list of dicts which describe all the desired
        column data sequential ordering (or the ORDER BY sql clause).

        example list of orderby dicts: 
        [
            {'name': 'group_id', 'order_by': 'desc'},
            {'name': 'created_at', 'order_by': 'desc'}
        ]

    """
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
        # if col_name is None or order_by is None:
        #     msg = 'ordering item missing either \'name\' or \'order_by\' - ' \
        #         f'ordering item: {value}.'
        #     raise KeyValue(msg)
        if order_by == ASCENDING:
            constructed_ordering.append(asc(col_obj))
        else:
            constructed_ordering.append(desc(col_obj))
    
    print(f'constructed_ordering: {constructed_ordering}')
    return constructed_ordering


@dataclass
class ExperimentRequest:
    request_dict: dict
    method: str = field(default_factory=str, init=False)
    params: dict = field(default_factory=dict, init=False)
    filters: dict = field(default_factory=dict, init=False)
    ordering: list = field(default_factory=list, init=False)
    record_limit: int = field(default_factory=int, init=False)
    body: dict = field(default_factory=dict, init=False)
    experiment: Experiment = field(init=False)
    experiment_data: namedtuple = field(init=False)
    response: dict = field(default_factory=dict, init=False)

    def __post_init__(self):
        method = self.request_dict.get('method')
        self.method = validate_method(method)
        self.params = self.request_dict.get('params')
        self.filters = None
        self.ordering = None
        self.record_limit = None

        if isinstance(self.params, dict):
            self.filters = construct_filters(self.params.get('filters'))
            self.ordering = self.params.get('ordering')
            self.record_limit = self.params.get('record_limit')
            if not type(self.record_limit) == int or self.record_limit <= 0:
                self.record_limit = None
                
        print(f'filters: {self.filters}')
        self.body = self.request_dict.get('body')
        if self.method == HTTP_PUT:
            self.experiment = get_experiment_from_body(self.body)
            # pprint(f'experiment: {repr(self.experiment)}')
            self.experiment_data = self.experiment.get_experiment_data()
            for k, v in zip(self.experiment_data._fields, self.experiment_data):
                val = pprint.pformat(v, indent=4)
                print(f'exp_data: k: {k}, v: {val}')
        

    def submit(self):

        if self.method == HTTP_GET:
            return self.get_experiments()
        elif self.method == HTTP_PUT:
            # becomes an update if record exists
            return self.put_experiment()

    
    def put_experiment(self):
        engine = stm.get_engine_from_settings()
        session = stm.get_session()

        record = exp(
            name=self.experiment_data.name,
            cycle_start=self.experiment_data.cycle_start,
            cycle_stop=self.experiment_data.cycle_stop,
            owner_id=self.experiment_data.owner_id,
            group_id=self.experiment_data.group_id,
            experiment_type=self.experiment_data.experiment_type,
            platform=self.experiment_data.platform,
            wallclock_start=self.experiment_data.wallclock_start,
            wallclock_end=self.experiment_data.wallclock_end,
            description=self.experiment_data.description,
            created_at=datetime.utcnow(),
            updated_at=datetime.utcnow()
        )

        insert_stmt = insert(exp).values(
            name=self.experiment_data.name,
            cycle_start=self.experiment_data.cycle_start,
            cycle_stop=self.experiment_data.cycle_stop,
            owner_id=self.experiment_data.owner_id,
            group_id=self.experiment_data.group_id,
            experiment_type=self.experiment_data.experiment_type,
            platform=self.experiment_data.platform,
            wallclock_start=self.experiment_data.wallclock_start,
            wallclock_end=self.experiment_data.wallclock_end,
            description=self.experiment_data.description,
            created_at=datetime.utcnow(),
            updated_at=None
        ).returning(exp)
        print(f'insert_stmt: {insert_stmt}')

        time_now = datetime.utcnow()

        do_update_stmt = insert_stmt.on_conflict_do_update(
            constraint='unique_experiment',
            set_=dict(
                # group_id=self.experiment_data.group_id,
                wallclock_end=time_now,
                updated_at=time_now
            )
        )

        print(f'do_update_stmt: {do_update_stmt}')

        
        # temp = vars(result._metadata)
        # print(f'result.inserted_primary_key: {result.inserted_primary_key}')
        # for item in temp:
        #     print(item, ':', temp[item])

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
            message = f'Attempt to {action} experiment record FAILED'
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

    
    def get_experiments(self):
        engine = stm.get_engine_from_settings()
        session = stm.get_session()

        q = session.query(
            exp.id,
            exp.name,
            exp.cycle_start,
            exp.cycle_stop,
            exp.owner_id,
            exp.group_id,
            exp.experiment_type,
            exp.platform,
            exp.wallclock_start,
            exp.wallclock_end,
            exp.created_at,
            exp.updated_at
        ).select_from(
            exp
        )

        if self.filters is not None and len(self.filters) > 0:
            for key, value in self.filters.items():
                q = q.filter(value)
        
        # add column ordering
        column_ordering = build_column_ordering(exp, self.ordering)
        if column_ordering is not None and len(column_ordering) > 0:
            for ordering_item in column_ordering:
                q = q.order_by(ordering_item)

        # limit number of returned records
        if self.record_limit is not None:
            q = q.limit(self.record_limit)

        experiments = q.all()

        results = DataFrame()
        error_msg = None
        record_count = 0
        try:
            if len(experiments) > 0:
                results = DataFrame(experiments, columns = experiments[0]._fields)
            
        except Exception as err:
            message = 'Request for experiment records FAILED'
            error_msg = f'Failed to get experiment records - err: {err}'
        else:
            message = 'Request for experiment records SUCCEEDED'
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
