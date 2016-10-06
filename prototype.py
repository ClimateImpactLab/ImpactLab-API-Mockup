
# compare the latex outputs to the math presented in the NAS presentation
# http://sites.nationalacademies.org/cs/groups/dbassesite/documents/webpage/dbasse_172599.pdf

	
import xarray as xr, pandas as pd, numpy as np
import json, numpy, hashlib
from IPython import get_ipython
from IPython.display import display, Markdown, Latex, FileLink
import ipywidgets.widgets as widgets

class Variable(object):
	'''
	Climate Impact Lab variable class

	Parameters
	----------
	value : coercable to xarray.DataArrays
		The underlying data set.

	symbolic : str (optional)
		Latex representation. For this example, assume we can pull 'symbol' 
		from attrs or use string representation of data.

	For math operations, this class uses the underlying 'value' attributes to do 
	the computation. Therefore, if we configure the objects to use dask rather 
	than in-memory, and figure out how to do the dask distributed computing, 
	this configuration should work.

	'''
	def __init__(self, api, value, symbolic=None):
		self.api = api
		self.value=value

		if symbolic is None:
			if hasattr(value, 'attrs'):
				symbolic = value.attrs['latex'] + self.get_latex_dims()
			else:
				symbolic = str(value)


		self._symbolic=symbolic

	def __repr__(self):
		return self.value.__repr__()

	def _coerce(self, value):
		if not isinstance(value, Variable):
			return Variable(self.api, value)
		return value

	@property
	def attrs(self):
		return self.value.attrs

	@property
	def symbol(self):
		return self.attrs.get('latex', None)

	@symbol.setter
	def symbol(self, value):
		self.attrs['latex'] = value

	@property
	def attrs(self):
		return self.value.attrs

	@attrs.setter
	def attrs(self, value):
		self.value.attrs = value

	@property
	def symbolic(self):
		return self._symbolic


	@symbolic.setter
	def symbolic(self, value):
		 self._symbolic = '{}{}'.format(value, self.get_latex_dims())


	def __add__(self, other):
		other = self._coerce(other)
		return Variable(self.api, self.value + other.value, '{} + {}'.format(self.symbolic, other.symbolic))


	def __radd__(self, other):
		other = self._coerce(other)
		return Variable(self.api, other.value + self.value, '{} + {}'.format(other.symbolic, self.symbolic))


	def __iadd__(self, other):
		return self.__add__(other)


	def __sub__(self, other):
		other = self._coerce(other)
		return Variable(self.api, self.value - other.value, '{} - {}'.format(self.symbolic, other.symbolic))


	def __rsub__(self, other):
		other = self._coerce(other)
		return Variable(self.api, other.value - self.value, '{} - {}'.format(other.symbolic, self.symbolic))


	def __isub__(self, other):
		return self.__sub__(other)


	def __mul__(self, other):
		other = self._coerce(other)
		return Variable(self.api, self.value * other.value, '\\left({}\\right)\\left({}\\right)'.format(self.symbolic, other.symbolic))


	def __rmul__(self, other):
		other = self._coerce(other)
		return Variable(self.api, other.value * self.value, '\\left({}\\right)\\left({}\\right)'.format(other.symbolic, self.symbolic))


	def __imul__(self, other):
		return self.__mul__(other)


	def __div__(self, other):
		other = self._coerce(other)
		return Variable(self.api, self.value / other.value, '\\frac{{\\left({}\\right)}}{{\\left({}\\right)}}'.format(self.symbolic, other.symbolic))


	def __rdiv__(self, other):
		other = self._coerce(other)
		return Variable(self.api, other.value / self.value, '\\frac{{\\left({}\\right)}}{{\\left({}\\right)}}'.format(other.symbolic, self.symbolic))


	def __idiv__(self, other):
		return self.__div__(other)


	def __pow__(self, other):
		other = self._coerce(other)
		return Variable(self.api, self.value ** other.value, '{{\\left({}\\right)}}^{{\\left({}\\right)}}'.format(self.symbolic, other.symbolic))


	def __rpow__(self, other):
		other = self._coerce(other)
		return Variable(self.api, other.value ** self.value, '{{\\left({}\\right)}}^{{\\left({}\\right)}}'.format(other.symbolic, self.symbolic))


	def __ipow__(self, other):
		return self.__pow__(other)


	def sum(self, dim=None):
		return Variable(self.api, self.value.sum(dim=dim), '\\sum{}{{\\left\\{{{}\\right\\}}}}'.format(('_{{{}\in {}}}'.format(dim, dim.upper()) if dim is not None else ''), self.symbolic))

	def ln(self):
		return Variable(self.api, np.log(self.value), '\\ln{{\\left({}\\right)}}'.format(self.symbolic))

	def get_symbol(self):
		return self.attrs['latex'] + self.get_latex_dims()

	def get_latex_dims(self):
		return '_{{{}}}'.format(','.join(map(lambda d: self.api.dims[d]['latex'], self.value.dims)))

	def equation(self):
		try:
			symbol = self.symbol + self.get_latex_dims() + ' = '
		except:
			symbol = ''
		return '{}{}'.format(symbol, self.symbolic)
    
	def display(self):
		# if in_ipynb():
		display(Latex('\\begin{{equation}}\n{}\n\\end{{equation}}'.format(self.equation())))
		# else:
			# return '${}$'.format(self.equation())

	def compute(self):
		'''
		right now, this just returns the value
		'''

		return self.value

def get_random_variable(dims):
	data = np.random.random(tuple([len(d[1]) for d in dims]))
	foo = xr.DataArray(data, coords=dims)
	return foo.chunk(tuple(foo.shape))

def in_ipynb():
    try:
        cfg = get_ipython().config 
        if cfg['IPKernelApp']['parent_appname'] == 'ipython-notebook':
            return True
        else:
            return False
    except NameError:
        return False

def require(*kwargs):
	def get_decorator(func):
		def do_func(obj, var, **kwds):
			for kw in kwargs:
				if kw not in kwds:
					# if in_ipynb():
					# 	widgets.widget_string.Text()
					# else:
					arg = raw_input('{}: '.format(kw))
				kwds[kw] = arg

			func(obj, var, **kwds)
		return do_func
	return get_decorator


class ClimateImpactLabDataAPI(object):
	'''
	Implements the interface for Climate Impact Lab users
	'''

	REQUIRED = ['gcp_id','name','latex','description','author']
	''' Required list of variable arguments '''

	def __init__(self, *args, **kwargs):
		self._read_json_database()


	def _read_json_database(self):
		'''
		Provides dummy versions of the variables we need for this demo

		This method represents work that would be done beforehand. The data in 
		these variables should already be prepared in netCDF or csvv files. In 
		the production version, these datasets will also be probabilistic, and 
		climate variables will also be indexed by climate model.
		'''

		self.database = {}

		with open('database.json', 'r') as fp:
			ds = json.loads(fp.read())

		self.dims = ds['dims']

		for var in ds['variables']:
			data = np.ones(tuple([len(d.get('values', [0])) for d in ds['variables'][var]['dims']]))
			coords = [(ds['dims'][d['gcp_id']]['gcp_id'], d.get('values', [0])) for d in ds['variables'][var]['dims']]

			self.database[var] = Variable(self, xr.DataArray(data, coords=coords, attrs = ds['variables'][var]))
			self.database[var].value = self.database[var].value.chunk(tuple([1 for d in ds['variables'][var]['dims']]))
			
			self.database[var].latest = sorted(ds['variables'][var]['versions'].items(), key=lambda x: x[0])[0]


	def publish(self, variable, **kwargs):

		with open('database.json', 'r') as fp:
			ds = json.loads(fp.read())

		if 'gcp_id' in variable.attrs:
			gcp_id = variable.attrs['gcp_id']
		else:
			gcp_id = raw_input('{}: '.format('gcp_id'))
			variable.attrs['gcp_id'] = gcp_id

		if gcp_id in ds:
			raise KeyError('{} already in dataset'.format(gcp_id))

		ds['variables'][gcp_id] = {
			'uuid': hashlib.sha256(str(np.random.random())).hexdigest(),
			'updated': pd.datetime.now().strftime('%c'),
			'dims': [{'name': ds['dims'][d]['name'], 'gcp_id': ds['dims'][d]['gcp_id']} for d in variable.dims]
		}

		for attr in self.REQUIRED:
			ds['variables'][gcp_id][attr] = kwargs.get(attr, variable.attrs.get(attr, raw_input('{}: '.format(attr))))

		with open('database.json', 'w+') as fp:
			fp.write(json.dumps(ds, sort_keys=True, indent=4))

	def get_variable(self, varname):
		'''
		The actual API call. 
		'''

		return self.database[varname]

	def list_variables(self):
		return sorted(self.database.keys())

	def list_dims(self):
		return sorted(self.dims.keys())

	def configure(self, *args, **kwargs):
		print('API configuration updated')




