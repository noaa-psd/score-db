from collections import namedtuple
from dataclasses import dataclass, field
from datetime import datetime
import json
from db_action_response import DbActionResponse
import score_table_models as stm
from score_table_models import Region as rg

from pandas import DataFrame
from sqlalchemy.dialects.postgresql import insert
from sqlalchemy.inspection import inspect

MIN_LONG = -180.0
MAX_LONG = 180.0

HTTP_GET = 'GET'
HTTP_PUT = 'PUT'

PARAM_FILTER_TYPE = 'filter_type'

FILTER__NONE = 'none'
FILTER__BY_REGION_NAME = 'by_name'
FILTER__BY_REGION_DATA = 'by_data'

VALID_METHODS = [HTTP_GET, HTTP_PUT]
VALID_FILTER_TYPES = [
    FILTER__NONE, FILTER__BY_REGION_NAME, FILTER__BY_REGION_DATA]

EQUATORIAL = {'name': 'equatorial', 'min_lat': -5.0, 'max_lat': 5.0}
GLOBAL = {'name': 'global', 'min_lat': -90.0, 'max_lat': 90.0}
NORTH_HEMIS = {'name': 'north_hemis', 'min_lat': 20.0, 'max_lat': 60.0}
TROPICS = {'name': 'tropics', 'min_lat': -20.0, 'max_lat': 20.0}
SOUTH_HEMIS = {'name': 'south_hemis', 'min_lat': -60.0, 'max_lat': -20.0}
TEST_SOUTH_HEMIS = {'name': 'test_south_hemis', 'min_lat': -50.0, 'max_lat': -35.0}

DEFAULT_REGION_DEFS = [EQUATORIAL, GLOBAL, NORTH_HEMIS, TROPICS, SOUTH_HEMIS]

RegionData = namedtuple(
    'RegionData',
    [
        'name',
        'min_lat',
        'max_lat',
        'grid',
        'hash_val'
    ],
)

@dataclass
class Region:
    ''' region object storing region name and min/max latitude bounds '''
    name: str
    min_lat: float
    max_lat: float
    grid: str = field(default_factory=str, init=False)
    hash_val: str = field(default_factory=str, init=False)

    def __post_init__(self):
        if not isinstance(self.name, str):
            msg = f'name must be a string - name {self.name}'
            raise ValueError(msg)
        if (not isinstance(self.min_lat, float) or
            not isinstance(self.max_lat, float)):
            msg = f'min and max lat must be floats - min lat: {self.min_lat}' \
                f', max lat: {self.max_lat}'
            raise ValueError(msg)
        if self.min_lat > self.max_lat:
            msg = f'min_lat must be less than max_lat - ' \
                f'min_lat: {self.min_lat}, max_lat: {self.max_lat}'
            raise ValueError(msg)
        if self.max_lat < self.min_lat:
            msg = f'min_lat must be greater than min_lat - min_lat: {self.min_lat}, '\
                f'max_lat: {self.max_lat}'
            raise ValueError(msg)

        if abs(self.min_lat) > 90 or abs(self.max_lat) > 90:
            msg = f'min_lat or max_lat is out of allowed range, must be greater' \
                f' than -90 and let than 90 - min_lat: {self.min_lat}, ' \
                f'max_lat: {self.max_lat}'
            raise ValueError(msg)

        self.grid = 'POLYGON('
        self.grid = f'({float(MIN_LONG)} {float(self.max_lat)}),'
        self.grid += f'({float(MAX_LONG)} {float(self.max_lat)}),'
        self.grid += f'({float(MAX_LONG)} {float(self.min_lat)}),'
        self.grid += f'({float(MIN_LONG)} {float(self.min_lat)}),'
        self.grid += f'({float(MIN_LONG)} {float(self.max_lat)})'
        self.grid += ')'

        self.hash_val = self.name + self.grid

    
    def get_region_data(self):
        return RegionData(self.name, self.min_lat, self.max_lat, self.grid, self.hash_val)


DEFAULT_REGIONS = [
    Region(EQUATORIAL['name'], EQUATORIAL['min_lat'], EQUATORIAL['max_lat']),
    Region(GLOBAL['name'], GLOBAL['min_lat'], GLOBAL['max_lat']),
    Region(NORTH_HEMIS['name'], NORTH_HEMIS['min_lat'], NORTH_HEMIS['max_lat']),
    Region(TROPICS['name'], TROPICS['min_lat'], TROPICS['max_lat']),
    Region(SOUTH_HEMIS['name'], SOUTH_HEMIS['min_lat'], SOUTH_HEMIS['max_lat'])
]

DEFAULT_REGION_NAMES = [lambda x=x: x.name for x in DEFAULT_REGIONS]

def validate_list_of_regions(regions):
    if not isinstance(regions, list):
        raise TypeError(f'Must be list of Regions, actually {type(regions)}')
    
    unique_regions = set()
    try:
        for r in regions:
            validated_region = Region(
                r.get('name'), r.get('min_lat'), r.get('max_lat'))
            unique_regions.add(validated_region.get_region_data())
    except Exception as err:
        msg = f'problem parsing region data, regions: {regions}, err: {err}'
        raise TypeError(msg) from err
    
    return list(unique_regions)

def validate_list_of_strings(values):
    if not isinstance(values, list):
        raise TypeError(f'Must be list of strings, actually {type(values)}')
    if not all(isinstance(elem, str) for elem in values):
        raise TypeError(f'Not all members are strings - {values}')
    
    unique_string_list = set()
    for value in values:
        unique_string_list.add(value)
    
    return list(unique_string_list)

def validate_method(method):
    if method not in VALID_METHODS:
        msg = f'Request type must be one of: {VALID_METHODS}, actually: {method}'
        print(msg)
        raise ValueError(msg)
    
    return method


def validate_body(method, body, filter_type=None):
    
    if not isinstance(body, dict):
        msg = f'Request body must be a dict type, was: {type(body)}'
        raise TypeError(msg)
    
    region_names = None
    regions = None
    
    if method == HTTP_GET:
        if filter_type is None:
            raise ValueError('\'filter_type\' param must be specified.')
        
        if filter_type == FILTER__BY_REGION_NAME:
            region_names = validate_list_of_strings(body.get('regions'))
        elif filter_type == FILTER__BY_REGION_DATA:
            regions = validate_list_of_regions(body.get('regions'))
            region_names = [x.name for x in regions] 
        
    elif method == HTTP_PUT:
        regions = validate_list_of_regions(body.get('regions'))
        region_names = [x.name for x in regions]
    
    else:
        raise ValueError(f'Invalid method, must be one of {VALID_METHODS}')

    print(f'region_names: {region_names}, regions: {regions}')
    return [region_names, regions]


def get_filter_type(params):
    filter_type = params.get(PARAM_FILTER_TYPE)
    if filter_type is None:
        return FILTER__NONE
    elif filter_type not in VALID_FILTER_TYPES:
        raise ValueError(f'Invalid filter_type ({filter_type}). ' \
            f'Must be one of [{VALID_FILTER_TYPES}]')
    return filter_type

# def get_all_records(params):
#     get_all = params.get('all', None)
#     if get_all is None:
#         return False
#     if isinstance(get_all, bool):
#         return get_all
#     elif isinstance(get_all, int):
#         return bool(get_all)
#     elif not isinstance(get_all, str):
#         raise ValueError('Invalid type %r for option %s; use '
#                           '1/0, yes/no, true/false, on/off' % (
#                               get_all, 'get_all'))
#     elif get_all.lower() in ('1', 'yes', 'true', 't', 'y'):
#         return True
#     elif get_all.lower() in ('0', 'no', 'false', 'f', 'n'):
#         return False
#     else:
#         raise ValueError('Invalid value %r for option %s; use '
#                           '1/0, yes/no, true/false, y/n, t/f' % (
#                               get_all, 'get_all')) 





@dataclass
class RegionRequest:
    request_dict: dict
    method: str = field(default_factory=str, init=False)
    params: dict = field(default_factory=dict, init=False)
    filter_type: str = field(default_factory=str, init=False)
    body: dict = field(default_factory=dict, init=False)
    regions: list = field(default_factory=list, init=False)
    region_names: list = field(default_factory=list, init=False)
    response: dict = field(default_factory=dict, init=False)

    def __post_init__(self):
        self.method = validate_method(self.request_dict.get('method'))
        self.params = self.request_dict.get('params', {})
        self.filter_type = get_filter_type(self.params)
        self.body = self.request_dict.get('body')
        [self.region_names, self.regions] = validate_body(
            self.method, self.body, self.filter_type)
        

    def get_regions_hash_vals(self):
        hash_vals = []
        for region in self.regions:
            hash_vals.append(region.hash_val)
        return hash_vals

    def submit(self):
        engine = stm.get_engine_from_settings()
        session = stm.get_session()

        if self.method == HTTP_GET:
            error_msg = None
            message = None
            matched_json = None
            try:
                if self.filter_type == FILTER__NONE:
                    matched_records = self.get_regions()
                elif self.filter_type == FILTER__BY_REGION_NAME:
                    matched_records = self.get_regions_by_name()
                else:
                    hash_vals = self.get_regions_hash_vals()
                    if len(hash_vals) == 0:
                        hash_vals = None
                    matched_records = self.get_regions(hash_vals)
                message = f'Request returned {len(matched_records)} record/s'
                matched_json = matched_records.to_json(orient = 'records')
            except Exception as err:
                error_msg = f'Problems encountered requesting regions - {err}'
            print(f'matched_records: {matched_records}')
            response = DbActionResponse(
                self.request_dict,
                (error_msg is None),
                message,
                {
                    'matched_records': matched_json
                },
                error_msg
            )
            print(f'response: {response}')
            return response
        elif self.method == HTTP_PUT:
            hash_vals = self.get_regions_hash_vals()
            
            existing_regions = self.get_regions(hash_vals)
            print(f'existing_regions: {existing_regions}')

            records = []
            records_hash_vals = []
            # hash_val_list = existing_regions['unique_region'].tolist()
            # print(f'hash_val_list: {hash_val_list}')
            # insert all records which do not match existing records

            for region in self.regions:
                if (
                    existing_regions is None or
                    len(existing_regions) == 0 or
                    region.hash_val not in existing_regions['unique_region'].tolist()
                ):
                    item = rg(
                        name=region.name,
                        bounds=region.grid,
                        created_at=datetime.utcnow(),
                        updated_at=None
                    )
                    records.append(item)
                    records_hash_vals.append(region.hash_val)
            print(f'records: {records}')
            success = True
            error_msg = None
            record_count = 0
            try:
                inserted_records = DataFrame()
                if len(records) > 0:
                    session.bulk_save_objects(records)
                    session.commit()
                    inserted_records = self.get_region_set(records_hash_vals)

                message = f'Inserted {len(inserted_records)} new region/s.'
                print(f'inserts: {inserted_records}')
            except Exception as err:
                msg = f'Failed to insert {len(records)} records - err: {err}'
                print(msg)
                error_msg = msg
                message = f'Inserted {len(inserted_records)} new region/s.'
            
            matched_records = self.get_regions(hash_vals)

            inserts_json = inserted_records.to_json(orient = 'records')
            matched_json = matched_records.to_json(orient = 'records')

            response = DbActionResponse(
                self.request_dict,
                (error_msg is None),
                message,
                {
                    'inserted_records': inserts_json,
                    'matched_records': matched_json
                },
                error_msg
            )
            print(f'response: {response}')
            return response



    def get_regions_by_name(self):
        engine = stm.get_engine_from_settings()
        session = stm.get_session()
        try:
            existing_regions = session.query(
                rg.id,
                rg.name,
                rg.bounds,
                rg.created_at,
                rg.name.concat(rg.bounds).label('unique_region')
            ).select_from(
                rg
            ).filter(
                rg.name.in_(self.region_names)
            ).all()
        except Exception as err:
            msg = f'Problem requesting region set - err: {err}'
            print(msg)
            return DataFrame()

        session.close()
        if len(existing_regions) == 0:
            return DataFrame()

        return DataFrame(existing_regions, columns = existing_regions[0]._fields)

    def get_all_regions(self):
        engine = stm.get_engine_from_settings()
        session = stm.get_session()
        try:
            existing_regions = session.query(
                rg.id,
                rg.name,
                rg.bounds,
                rg.created_at,
                rg.name.concat(rg.bounds).label('unique_region')
            ).select_from(
                rg
            ).all()
        except Exception as err:
            msg = f'Problem requesting region set - err: {err}'
            print(msg)
            return DataFrame()

        session.close()
        if len(existing_regions) == 0:
            return DataFrame()

        return DataFrame(existing_regions, columns = existing_regions[0]._fields)
    
    def get_region_set(self, hash_vals):
        if not isinstance(hash_vals, list):
            msg = f'requested set must be in form of list - {type(hash_vals)}'
            raise TypeError()

        engine = stm.get_engine_from_settings()
        session = stm.get_session()
        print(f'hash_vals: {hash_vals}')
        # if no regions were specified in query, get all

        try:
            existing_regions = session.query(
                rg.id,
                rg.name,
                rg.bounds,
                rg.created_at,
                rg.name.concat(rg.bounds).label('unique_region')
            ).select_from(
                rg
            ).filter(
                rg.name.concat(rg.bounds).in_(hash_vals)
            ).all()
        except Exception as err:
            msg = f'Problem requesting region set - err: {err}'
            print(msg)
            return DataFrame()

        # print(f'labels for existing_regions: {existing_regions[0]._fields}')
        session.close()
        if len(existing_regions) == 0:
            return DataFrame()

        return DataFrame(existing_regions, columns = existing_regions[0]._fields)


    def get_regions(self, hash_vals=None):
        if hash_vals is None:
            df = self.get_all_regions()
        else:
            df = self.get_region_set(hash_vals)
            

            
        print(f'df: {df}')
        return df









        
        
