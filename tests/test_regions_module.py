"""
Copyright 2022 NOAA
All rights reserved.

Unit tests for io_utils

"""
import os
import pathlib
import pytest

import score_table_models as scr_models
from score_table_models import Region as regions_table
import regions as rgs
from regions import RegionData, Region, RegionRequest

from score_db_base import handle_request



# scr_models.init_tables()

def test_validate_method():
    with pytest.raises(ValueError):
        rgs.validate_method('blah')
    
    with pytest.raises(ValueError):
        rgs.validate_method(None)
    
    with pytest.raises(ValueError):
        rgs.validate_method([])
    
    for method in rgs.VALID_METHODS:
        rgs.validate_method(method)


def test_validate_list_of_strings():
    with pytest.raises(TypeError):
        rgs.validate_list_of_strings({})

    with pytest.raises(TypeError):
        rgs.validate_list_of_strings(None)

    with pytest.raises(TypeError):
        rgs.validate_list_of_strings('dude')

    with pytest.raises(TypeError):
        rgs.validate_list_of_strings([1, 5, 6])

    region_list = ['foo', 'bar', 'foo']
    output_list = rgs.validate_list_of_strings(region_list)
    for output in output_list:
        assert output_list.count(output) == 1


def test_validate_list_of_regions():
    with pytest.raises(TypeError):
        rgs.validate_list_of_regions(None)

    with pytest.raises(TypeError):
        rgs.validate_list_of_regions({})

    with pytest.raises(TypeError):
        rgs.validate_list_of_regions('foo')

    with pytest.raises(TypeError):
        rgs.validate_list_of_regions(1)

    region_list = [rgs.EQUATORIAL, rgs.GLOBAL, rgs.SOUTH_HEMIS, rgs.GLOBAL]
    validated_regions = rgs.validate_list_of_regions(region_list)
    print(f'validated_regions: {validated_regions}')
    for region in validated_regions:
        assert validated_regions.count(region) == 1
        assert isinstance(region, RegionData)


def test_validate_body():
    with pytest.raises(TypeError):
        rgs.validate_body(rgs.HTTP_GET, None)

    with pytest.raises(TypeError):
        rgs.validate_body(rgs.HTTP_GET, None)

    with pytest.raises(TypeError):
        rgs.validate_body(rgs.HTTP_GET, [])

    with pytest.raises(TypeError):
        rgs.validate_body(rgs.HTTP_GET, 'foo')

    with pytest.raises(TypeError):
        rgs.validate_body(rgs.HTTP_GET, 1)
    

    body = {'regions': {}}
    with pytest.raises(TypeError):
        rgs.validate_body(rgs.HTTP_GET, body, rgs.FILTER__BY_REGION_NAME)
    
    body = {'regions': [1, 2, 9]}
    with pytest.raises(TypeError):
        rgs.validate_body(rgs.HTTP_GET, body, rgs.FILTER__BY_REGION_NAME)

    body = {'regions': ['foo', 'bar', 'foo']}
    [region_names, regions] = rgs.validate_body(rgs.HTTP_GET, body, rgs.FILTER__BY_REGION_NAME)
    for name in region_names:
        assert region_names.count(name) == 1
    
    assert regions is None

    body = {'regions': [rgs.GLOBAL, rgs.EQUATORIAL, rgs.GLOBAL]}
    [region_names, regions] = rgs.validate_body(rgs.HTTP_PUT, body)
    for name in region_names:
        assert region_names.count(name) == 1
        print(f'name: {name}')
    
    for region in regions:
        assert regions.count(region) == 1
        assert isinstance(region, RegionData)


def test_initialize_region_request_prep():

    request_dict = {
        'name': 'region',
        'method': 'PUT',
        'body': {
            'regions': [
                rgs.GLOBAL,
                rgs.EQUATORIAL,
                rgs.NORTH_HEMIS,
                rgs.SOUTH_HEMIS,
                rgs.TROPICS,
                rgs.GLOBAL
            ]
        }
    }

    rr = RegionRequest(request_dict)
    print(f'rr_prep: {rr}')
    
    for name in rr.region_names:
        print(f'region: {name}')

# def test_get_all_records_param_check():
#     params_forced_true = [
#         {'all': True},
#         {'all': 'true'},
#         {'all': 'TRUE'},
#         {'all': 't'},
#         {'all': 'T'},
#         {'all': 'yes'},
#         {'all': 'YES'},
#         {'all': 'Y'}
#     ]

#     for params in params_forced_true:
#         print(f'params: {params}')
#         assert rgs.get_all_records(params)
    
#     params_forced_false = [
#         {},
#         {'all': 0},
#         {'all': 'F'},
#         {'all': 'no'},
#         {'all': 'N'}
#     ]

#     for params in params_forced_false:
#         assert not rgs.get_all_records(params)

    
#     params_forced_invalid = [
#         {'all': 'dude'},
#         {'all': []},
#         {'all': {}},
#     ]

#     for params in params_forced_invalid:
#         with pytest.raises(ValueError):
#             rgs.get_all_records(params)


def test_request_put_regions():
    request_dict = {
        'name': 'region',
        'method': 'PUT',
        'body': {
            'regions': [
                rgs.GLOBAL,
                rgs.EQUATORIAL,
                rgs.NORTH_HEMIS,
                rgs.SOUTH_HEMIS,
                rgs.TROPICS,
                rgs.GLOBAL,
                rgs.TEST_SOUTH_HEMIS
            ]
        }
    }

    rr = RegionRequest(request_dict)
    rr.submit()

def test_request_get_specific_regions_by_name():
    request_dict = {
        'name': 'region',
        'method': 'GET',
        'params': {'filter_type': 'by_name'},
        'body': {
            'regions': [
                rgs.GLOBAL.get('name'),
                rgs.EQUATORIAL.get('name'),
                rgs.NORTH_HEMIS.get('name'),
                rgs.SOUTH_HEMIS.get('name'),
            ]
        }
    }

    rr = RegionRequest(request_dict)
    rr.submit()

def test_request_get_specific_regions_by_region_data():
    request_dict = {
        'name': 'region',
        'method': 'GET',
        'params': {'filter_type': 'by_data'},
        'body': {
            'regions': [
                rgs.GLOBAL,
                rgs.EQUATORIAL,
                rgs.NORTH_HEMIS,
                rgs.SOUTH_HEMIS,
            ]
        }
    }

    rr = RegionRequest(request_dict)
    rr.submit()


def test_request_all_regions():
    request_dict = {
        'name': 'region',
        'method': 'GET',
        'params': {'filter_type': 'none'},
        'body': {
            'regions': [
                rgs.GLOBAL,
                rgs.EQUATORIAL,
                rgs.NORTH_HEMIS,
                rgs.SOUTH_HEMIS,
            ]
        }
    }

    rr = RegionRequest(request_dict)
    rr.submit()


    


