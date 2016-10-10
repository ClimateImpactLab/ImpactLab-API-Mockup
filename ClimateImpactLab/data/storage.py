
import xarray as xr, pandas as pd, numpy as np, os
import json, numpy, hashlib
from IPython import get_ipython
from IPython.display import display, Markdown, Latex, FileLink
import ipywidgets.widgets as widgets
import warnings

from ..api.variable import Variable, _VariableGetter
from ..utils import _compat
from ..utils.misc import _require
from ..utils.exceptions import ConnectionError

class MetaDatabase(object):

    def __init__(self, s3):
        self._database = {dtype:{} for dtype in ['variables', 'files', 'dims', 'functions', 'scenarios']}
        self._s3 = s3

    @property
    def variables(self):
        return self._database['variables']

    @variables.setter
    def variables(self, value):
        raise ValueError('Cannot assign to variables. See Client.publish().')

    @property
    def dims(self):
        return self._database['dims']

    @dims.setter
    def dims(self, value):
        raise ValueError('Cannot assign to dims. See Client.create_variable_dim().')

    @property
    def files(self):
        return self._database['files']

    @files.setter
    def files(self, value):
        raise ValueError('Cannot assign to files. See Client.upload_file().')

    @property
    def functions(self):
        return self._database['functions']

    @functions.setter
    def functions(self, value):
        raise ValueError('Cannot assign to functions. See Client.define()')

    @property
    def scenarios(self):
        return self._database['scenarios']

    @scenarios.setter
    def scenarios(self, value):
        raise ValueError('Cannot assign to scenarios. See Client.create_scenario()')


    def update_database(self):
        '''
        Update data in self._database from AWS
        '''

        raise NotImplementedError()

    def commit_changes(self):
        '''
        Commit published local changes to AWS
        '''

        raise NotImplementedError()

    def upload_file(self, filepath):
        '''
        Document and upload file to AWS from filepath
        '''

        



class MockupDatabase(MetaDatabase):
    '''
    TODO
    ----
    The model of loading ready-to-use variables into memory for all variables is obviously unteneble...

    '''

    LOCAL_JSON_DB = 'ClimateImpactLab/data/database.json'
    ''' filepath to local JSON DB'''

    REMOTE_JSON_DB_NAME = 'mockup_api_database_01.json'
    ''' name of remote JSON DB object '''

    REMOTE_JSON_DB_BUCKET = 'impactlab-meta'
    ''' name of remote JSON DB bucket '''

    def __init__(self, *args, **kwargs):
        super(MockupDatabase, self).__init__(*args, **kwargs)

    def update_database(self):
        '''
        Update data in self._database from AWS
        '''
        
        try:
            updates = json.loads(self._s3.download(self.REMOTE_JSON_DB_BUCKET,self.REMOTE_JSON_DB_NAME)['Body'].read())
        except ConnectionError:
            warnings.warn('Connection to database could not be established. Using Local database.', RuntimeWarning)
            updates = self._read_local_database()

        updated_db = self._generate_database(updates)
        self._merge_database(updated_db)
        self._write_local_database()

    def commit_changes(self):
        '''
        Commit published local changes to AWS
        '''

        self.update_database()
        self._write_local_database()
        self._s3.upload(self.REMOTE_JSON_DB_BUCKET, self.REMOTE_JSON_DB_NAME, self.LOCAL_JSON_DB)

    def _serialize_db(self):
        database = {}

        for dtype, data in self._database.items():
            if dtype == 'variables':
                database[dtype] = {k: v.attrs for k, v in data.items()}
            else:
                database[dtype] = data

        return json.dumps(database, sort_keys=True, indent=4)

    def _write_local_database(self):

        with open(self.LOCAL_JSON_DB, 'w+') as fp:
            fp.write(self._serialize_db())

    def _read_local_database(self):
        with open(self.LOCAL_JSON_DB, 'r') as fp:
            ds = json.loads(fp.read())

        return ds

    def _generate_database(self, data):
        '''
        Creates Variable objects from variabls listed in the databse
        '''

        # dataset to create from ``data``
        dataset = {}

        for datatype in data:
            
            # skip variables -- we'll add these later
            if datatype == 'variables':
                continue

            dataset[datatype] = data[datatype]

        dataset['variables'] = {}

        for var in data['variables']:
            array = np.ones(tuple([len(d.get('values', [0])) for d in data['variables'][var]['dims']]))
            coords = [(data['dims'][d['gcp_id']]['gcp_id'], d.get('values', [''])) for d in data['variables'][var]['dims']]
            derived = data['variables'][var].get('derived', True)

            dataset['variables'][var] = Variable(api=self, value=xr.DataArray(array, coords=coords, attrs = data['variables'][var]), derived=derived, derivation=data['variables'][var].get('derivation', ''))
            dataset['variables'][var].value = dataset['variables'][var].value.chunk(tuple([1 for d in data['variables'][var]['dims']]))
            
        return dataset

    def _merge_database(self, database):
        '''
        Merge databases, updating with new information but throwing errors on conflicts

        please oh please find a better way of doing this...
        '''

        for dtype in database:
            if dtype != 'variables':
                self._database[dtype].update(database[dtype])
                continue

            for var in database['variables']:
                if var not in self.variables:
                    self.variables[var] = database['variables'][var]

                else:
                    for key, vals in database['variables'][var].attrs.items():
                        if key == 'versions':
                            for version, versioned_obj in vals.items():
                                if version not in self.variables[var].attrs['versions']:
                                    self.variables[var].attrs['versions'][version] = versioned_obj
                                else:
                                    if not self.variables[var].attrs['versions'][version] == versioned_obj:
                                        raise ValueError('Variable version {} inconsistent with current version'.format(version))





    def publish(self, variable, **kwargs):

        def get_attr(attr):
            return kwargs.get(attr, variable.attrs.get(attr, raw_input('{}: '.format(attr))))

        gcp_id = get_attr('gcp_id')

        if gcp_id in ds:
            raise KeyError('{} already in dataset'.format(gcp_id))

        ds['variables'][gcp_id] = {
            'uuid': hashlib.sha256(str(np.random.random())).hexdigest(),
            'updated': pd.datetime.now().strftime('%c'),
            'dims': [{'name': ds['dims'][d]['name'], 'gcp_id': ds['dims'][d]['gcp_id']} for d in variable.value.dims]
        }

        for attr in self.REQUIRED:
            ds['variables'][gcp_id][attr] = get_attr(attr)

        updated = pd.timestamp.now().strftime('%Y-%m-%d %H:%M:%S')
        version = '{}.{}'.format(gcp_id, updated)
        ds['variables']['versions'] = {
            version : {
                'uuid': '',
                'version': version,
                'updated': updated,
                'dependencies': variable.dependencies,
                'filepath': ''
            }
        }
