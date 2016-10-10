
# compare the latex outputs to the math presented in the NAS presentation
# http://sites.nationalacademies.org/cs/groups/dbassesite/documents/webpage/dbasse_172599.pdf

    
import xarray as xr, pandas as pd, numpy as np, os
import json, numpy, hashlib
from IPython import get_ipython
from IPython.display import display, Markdown, Latex, FileLink
import ipywidgets.widgets as widgets

from .variable import Variable, _VariableGetter
from ..data import storage, s3
from ..utils import _compat
from ..utils.misc import _require

class Client(object):
    '''
    Implements the interface for Climate Impact Lab users
    '''

    REQUIRED = ['gcp_id','name','latex','description','author']
    ''' Required list of variable arguments '''

    def __init__(self, *args, **kwargs):
        self._aws_connection = s3.Griffin()
        self._database = storage.MockupDatabase(s3=self._aws_connection)

    def get_variable(self, varname):
        '''
        The actual API call. 
        '''

        return self._database.get_variable(varname)

    def refresh_database(self):
        self._database.update_database()
        self._update_variable_getter()
        self._update_file_getter()

    def commit(self):
        self._database.commit_changes()

    def _create_getter(self, data, datasource):
        for var in datasource:
            varcomponents = var.split('.')

            this = data

            for i, comp in enumerate(varcomponents[:-1]):
                if not comp in this.__dict__:
                    this.__dict__[comp] = _VariableGetter()
                this = this.__dict__[comp]

            this.__dict__[varcomponents[-1]] = _VariableGetter(datasource[var])
        

    def _update_variable_getter(self):
        '''
        Chop up available variables
        '''
        self.variables = _VariableGetter()
        self._create_getter(self.variables, self._database.variables)

    def _update_file_getter(self):
        '''
        Chop up available files
        '''
        self.files = _VariableGetter()
        self._create_getter(self.files, self._database.files)

    def list_variables(self):
        return sorted(map(lambda v: v.attrs['gcp_id'], self._database.variables.values()))

    def list_dims(self):
        return sorted(map(lambda v: v['gcp_id'], self._database.dims.values()))

    def list_files(self):
        return sorted(map(lambda v: v['gcp_id'], self._database.files.values()))

    def list_functions(self):
        return sorted(map(lambda v: v['gcp_id'], self._database.functions.values()))

    def list_scenarios(self):
        return sorted(map(lambda v: v['gcp_id'], self._database.scenarios.values()))

    def get_variable(self, key):
        return self._database.variables[key]

    def get_dim(self, key):
        return self._database.dims[key]

    def get_file(self, key):
        return self._database.files[key]

    def get_function(self, key):
        return self._database.functions[key]

    def get_scenario(self, key):
        return self._database.scenarios[key]

    def configure(self, *args, **kwargs):
        print('API configuration updated')

    def upload_file(self, filepath):
        raise NotImplementedError()


