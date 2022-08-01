# score-db
Python package to manage the database backend to support the UFS-RNR workflow
experiment meta data and analyses data tracking and reporting.  The score-db
database uses the PostgreSQL database system which is hosted by AWS on an RDS
instance (currently administered by PSL).

Tools and utilities to catalogue and evaluate observation files.

# Installation and Environment Setup
1. Clone the score-db and score-hv repos.  Both of these repos are required.
score-db is responsible for managing the backend database which stores 
diagnostics data related to UFS-RNR experiments.  score-db has several APIs
to help users insert and collect data from the score-db database.  score-hv,
on the other hand, is responsible for harvesting data from the diagnostic

```sh
$ git clone https://github.com/noaa-psd/score-db.git
$ git clone https://github.com/noaa-psd/score-hv.git
```

2. Install or setup the UFS-RNR anaconda3 python environment and update it. 
If the UFS-RNR anaconda python environment is not already available, install
it now using the instructions found in the [UFS-RNR-stack](https://github.com/HenryWinterbottom-NOAA/UFS-RNR-stack.git) repo.  An example of the actual
installation script can be found at [scripts/build.UFS-RNR-stack.RDHPCS-Hera.anaconda3.sh](https://github.com/HenryWinterbottom-NOAA/UFS-RNR-stack/blob/0b0fd767928ebc56be0c1992b141015d9e3ff7a4/scripts/build.UFS-RNR-stack.RDHPCS-Hera.anaconda3.sh).
If you are installing this anaconda module at a different location than what
is configured in the above script, you will need to make modifications such
that the install location matches your desired location.  You will also need to
specify aws credentials for PSL's private s3 bucket.
3. Update the anaconda environment (follow anaconda update directions found [here](https://docs.anaconda.com/anaconda/install/update-version/) or use
the example shown below).  Note: if you do not specify the anaconda environment to
update, the `conda update --all` command will update the current environment. The
`--all` indicates that you want to update all packages.  In order to specify a
particular environment, use the command `conda update -n myenv --all`.

```sh
$ conda update conda
$ conda update --all
```

4. Load the anaconda3 environment
```sh
$ module purge
$ module use -a /contrib/home/builder/UFS-RNR-stack/modules
$ module load anaconda3
```
5. Make the python interpreter aware of where the score-db and score-hv
source code can be found.

```sh
$ export SCORE_DB_HOME_DIR=$( cd -- "$( dirname -- "${BASH_SOURCE[0]}" )" &> /dev/null && pwd )
$ export PYTHONPATH=$SCORE_DB_HOME_DIR/src
$ export PYTHONPATH=$PYTHONPATH:[absolute or relative path to score-hv]/src
```

6. Configure the PostgreSQL credentials and settings for the score-db by
creating a `.env` file and by inserting the text shown below (note: this
text is taken straight from the file `.env_example`).  You will need to 
acquire the database password from the administrator (Sergey Frolov).

```
from the score-db repo top level, cat the example file
$ cat .env_example

SCORE_POSTGRESQL_DB_NAME = 'rnr_scores_db'
SCORE_POSTGRESQL_DB_PASSWORD = '[score_db_password]'
SCORE_POSTGRESQL_DB_USERNAME = 'ufsrnr_user'
SCORE_POSTGRESQL_DB_ENDPOINT = 'ufsrnr-pg-db.cmpwyouptct1.us-east-2.rds.amazonaws.com'
SCORE_POSTGRESQL_DB_PORT = 5432
```

# Using the APIs to Interact with score-db
Each of the APIs is structured in a similar way and are meant to be
accessible via either a direct library call or via a command line call
with a yaml file or a python dictionary as the only accepted arguments.

#### How to Register an Experiment
This API helps the user register an experiment's meta data into the score-db
`experiments` table.

1. Setup the experiment registration yaml file.

example experiment registration yaml
```
---
name: experiment
method: PUT
body:
  name: EXAMPLE_EXPERIMENT_C96L64.UFSRNR.GSI_3DVAR.012016
  datestr_format: "%Y-%m-%d %H:%M:%S"
  cycle_start: '2016-01-01 00:00:00'
  cycle_stop: '2016-01-31 18:00:00'
  owner_id: first.last@noaa.gov
  group_id: gsienkf
  experiment_type: C96L64.UFSRNR.GSI_3DVAR.012016
  platform: pw_awv1
  wallclock_start: '2021-07-22 09:22:05'
  wallclock_end: '2021-07-24 05:31:14'
  description: '{"unstructured_json": "data"}'
```
2. Use the command line to execute the API and register the experiment.

```sh
$ python3 src/score_db_base.py tests/experiment_registration__valid.yaml
```

3. Call the experiment registration task from a python script.
```
import json
import experiments as expts
from experiments import ExperimentData, Experiment, ExperimentRequest

def register_experiment():
    with open(EXPERIMENT_CONFIG_FILE, 'r') as config_file:
        data=config_file.read()
    
    description = json.loads(data)

    request_dict = {
        'name': 'experiment',
        'method': 'PUT',
        'body': {
            'name': 'C96L64.UFSRNR.GSI_3DVAR.012016',
            'datestr_format': '%Y-%m-%d %H:%M:%S',
            'cycle_start': '2016-01-01 00:00:00',
            'cycle_stop': '2016-01-31 18:00:00',
            'owner_id': 'Steve.Lawrence@noaa.gov',
            'group_id': 'gsienkf',
            'experiment_type': 'C96L64.UFSRNR.GSI_3DVAR.012016',
            'platform': 'pw_awv1',
            'wallclock_start': '2021-07-22 09:22:05',
            'wallclock_end': '2021-07-24 05:31:14',
            'description': json.dumps(description)
        }
    }

    er = ExperimentRequest(request_dict)
    response = er.submit()
    return response

response = register_experiment()
```

Note: the `EXPERIMENT_CONFIG_FILE` is meant to contain a json version of the
experiment configuration.  PostgreSQL allows searching through unstructured
JSON ([JSON Functions and Operators](https://www.postgresql.org/docs/12/functions-json.html) and [PostgreSQL JSON query tutorial](https://www.postgresqltutorial.com/postgresql-tutorial/postgresql-json/) should
help the user understand how to query data stored in the json `description`
column)

#### How to Harvest Innovation Stats Produced by the UFS-RNR Workflow
This API will collect innovation statistics diagnostic data produced by the
UFS-RNR workflow.  This diagnostics data consists of bias, rmsd, and count
statistics of temperature, specific humidity, and uv wind and is stored in
netcdf files.  The harvester engine pulls this data out of the netcdf files
and inserts it into the score-db database.  The user must know the experiment
name and start wallclock time so that the API can associate the statistics
data with the correct registered experiment.  Note: the experiment must be
registered prior to any attempt to harvest the data.  Additionally, the netcdf
files must also already be downloaded and stored locally.


1. Setup the yaml input argument.

example of the input yaml (one can cut and paste this yaml data and)
```
date_range:
  datetime_str: '%Y-%m-%d %H:%M:%S'
  end: '2016-01-31 18:00:00'
  start: '2016-01-01 00:00:00'
db_request_name: harvest_innov_stats
expt_name: C96L64.UFSRNR.GSI_3DVAR.012016
expt_wallclk_strt: '2021-07-22 09:22:05'
files:
- cycles:
  - 0
  - 21600
  - 43200
  - 64800
  elevation_unit: plevs
  filename: innov_stats.metric.%Y%m%d%H.nc
  filepath: /home/slawrence/Development/experiment_data/uncoupled_c96_3dvar/gsi/
  harvester: innov_stats_netcdf
  metrics:
  - temperature
  - spechumid
  - uvwind
  stats:
  - bias
  - count
  - rmsd
output_format: tuples_list

```

2. Call the harvester engine on the command line (see below).  Both command
lines require mostly the same syntax `python3 src/score_db_base.py [command argument yaml]`
```sh
$ python3 src/score_db_base.py tests/netcdf_harvester_config__valid.yaml
```

or one could issue the command from within a python script.
```sh
from harvest_innov_stats import HarvestInnovStatsRequest

harvester_control_dict = {
  "date_range": {
    "datetime_str": "%Y-%m-%d %H:%M:%S",
    "end": "2016-01-31 18:00:00",
    "start": "2016-01-01 00:00:00"
  },
  "db_request_name": "harvest_innov_stats",
  "expt_name": "C96L64.UFSRNR.GSI_3DVAR.012016",
  "expt_wallclk_strt": "2021-07-22 09:22:05",
  "files": [
    {
      "cycles": [
        0,
        21600,
        43200,
        64800
      ],
      "elevation_unit": "plevs",
      "filename": "innov_stats.metric.%Y%m%d%H.nc",
      "filepath": "/home/slawrence/Development/experiment_data/uncoupled_c96_3dvar/gsi/",
      "harvester": "innov_stats_netcdf",
      "metrics": [
        "temperature",
        "spechumid",
        "uvwind"
      ],
      "stats": [
        "bias",
        "count",
        "rmsd"
      ]
    }
  ],
  "output_format": "tuples_list"
}

hc = HarvestInnovStatsRequest(harvester_control_dict)
hc.submit()
```

#### How to Plot Innovation Statistics
This API will collect innovation statistics produced by one or more UFS-RNR
experiments.  The statistics must already be inserted into the `expt_metrics`
table and the experiments must already be registered.

1. Setup the yaml input argument.

example of the input yaml (one can cut and paste this yaml data and)
```
date_range:
  datetime_str: '%Y-%m-%d %H:%M:%S'
  end: '2016-01-31 18:00:00'
  start: '2016-01-01 00:00:00'
db_request_name: plot_innov_stats
experiments:
- graph_color: blue
  graph_label: C96L64 GSI Uncoupled 3DVAR Experiment
  name: C96L64.UFSRNR.GSI_3DVAR.012016
  wallclock_start: '2021-07-22 09:22:05'
- graph_color: red
  graph_label: C96L64 GSI and SOCA Coupled 3DVAR Experiment
  name: C96L64.UFSRNR.GSI_SOCA_3DVAR.012016
  wallclock_start: '2021-07-24 11:31:16'
fig_base_fn: C96L64_GSI_3DVAR_VS_GSI_SOCA_3DVAR
stat_groups:
- cycles:
  - 0
  - 21600
  - 43200
  - 64800
  elevation_unit: plevs
  metrics:
  - temperature
  - spechumid
  - uvwind
  regions:
  - equatorial
  - global
  - north_hemis
  - south_hemis
  - tropics
  stat_group_frmt_str: innov_stats_{metric}_{stat}
  stats:
  - bias
  - rmsd
work_dir: /absolute/path/to/desired/figure/location
```

2. Call the harvester engine on the command line (see below).  Both command
lines require mostly the same syntax `python3 src/score_db_base.py [command argument yaml]`
```sh
$ python3 src/score_db_base.py tests/plot_innov_stats_config__valid.yaml
```

or one could issue the command from within a python script.
```
from plot_innov_stats import PlotInnovStatsRequest

def plot_innov_stats_for_date_range():

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
            }
        ],
        'work_dir': '/absolute/path/to/desired/figure/location',
        'fig_base_fn': 'C96L64_GSI_3DVAR_VS_GSI_SOCA_3DVAR'
    }

    plot_request = PlotInnovStatsRequest(plot_control_dict)
    plot_request.submit()

plot_innov_stats_for_date_range()
```

# Table Schemas

The score-db backend package is comprised of four tables and several APIs
which are meant to help the user insert and select data from the score-db
database.  Instructions for API use are shown below.  This section describes
what information is stored in each table.  
`cmd_results`.


```sh
experiments

    id = Column(Integer, primary_key=True, autoincrement=True)
    name = Column(String(128), nullable=False)
    cycle_start = Column(DateTime, nullable=False)
    cycle_stop = Column(DateTime, nullable=False)
    owner_id = Column(String(64), nullable=False)
    group_id = Column(String(16))
    experiment_type = Column(String(64))
    platform = Column(String(16), nullable=False)
    wallclock_start = Column(DateTime, nullable=False)
    wallclock_end = Column(DateTime)
    description = Column(JSONB(astext_type=sa.Text()), nullable=True)
    created_at = Column(DateTime)
    updated_at = Column(DateTime)
```

```sh
regions

    id = Column(Integer, primary_key=True, autoincrement=True)
    name = Column(String(79), nullable=False)
    bounds = Column(String(255), nullable=False)
    created_at = Column(DateTime, nullable=False)
    updated_at = Column(DateTime)

    metrics = relationship('ExperimentMetric', back_populates='region')
```

```sh
metric_types

    id = Column(Integer, primary_key=True, autoincrement=True)
    name = Column(String(128), nullable=False)
    measurement_type = Column(String(64), nullable=False)
    measurement_units = Column(String(64))
    stat_type = Column(String(64))
    description = Column(JSONB(astext_type=sa.Text()), nullable=True)
    created_at = Column(DateTime, nullable=False)
    updated_at = Column(DateTime)
    
    metrics = relationship('ExperimentMetric', back_populates='metric_type')
```

```sh
expt_metrics

    id = Column(Integer, primary_key=True, autoincrement=True)
    experiment_id = Column(Integer, ForeignKey('experiments.id'))
    metric_type_id = Column(Integer, ForeignKey('metric_types.id'))
    region_id = Column(Integer, ForeignKey('regions.id'))
    elevation = Column(Float, nullable=False)
    elevation_unit = Column(String(32))
    value = Column(Float)
    time_valid = Column(DateTime, nullable=False)
    created_at = Column(DateTime, default=datetime.utcnow())

    experiment = relationship('Experiment', back_populates='metrics')
    metric_type = relationship('MetricType', back_populates='metrics')
    region = relationship('Region', back_populates='metrics')
```
