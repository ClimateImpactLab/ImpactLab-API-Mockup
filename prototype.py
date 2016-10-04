
# compare the latex outputs to the math presented in the NAS presentation
# http://sites.nationalacademies.org/cs/groups/dbassesite/documents/webpage/dbasse_172599.pdf

	
import xarray as xr, pandas as pd, numpy as np
import json, numpy, hashlib
from IPython.display import display, Markdown, Latex


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
	def __init__(self, value, symbolic=None):
		self.value=value

		if symbolic is None:
			if hasattr(value, 'attrs'):
				symbolic = value.attrs['latex'] + '_{{{}}}'.format(','.join(value.dims))
			else:
				symbolic = str(value)


		self._symbolic=symbolic

	def __repr__(self):
		return self.value.__repr__()

	@staticmethod
	def _coerce(value):
		if not isinstance(value, Variable):
			return Variable(value)
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
	def symbolic(self):
		return self._symbolic


	@symbolic.setter
	def symbolic(self, value):
		 self._symbolic = '{}_{{{}}}'.format(value, ','.join(self.value.dims))


	def __add__(self, other):
		other = self._coerce(other)
		return Variable(self.value + other.value, '{} + {}'.format(self.symbolic, other.symbolic))


	def __radd__(self, other):
		other = self._coerce(other)
		return Variable(other.value + self.value, '{} + {}'.format(other.symbolic, self.symbolic))


	def __iadd__(self, other):
		return self.__add__(other)


	def __sub__(self, other):
		other = self._coerce(other)
		return Variable(self.value - other.value, '{} - {}'.format(self.symbolic, other.symbolic))


	def __rsub__(self, other):
		other = self._coerce(other)
		return Variable(other.value - self.value, '{} - {}'.format(other.symbolic, self.symbolic))


	def __isub__(self, other):
		return self.__sub__(other)


	def __mul__(self, other):
		other = self._coerce(other)
		return Variable(self.value * other.value, '\\left({}\\right)\\left({}\\right)'.format(self.symbolic, other.symbolic))


	def __rmul__(self, other):
		other = self._coerce(other)
		return Variable(other.value * self.value, '\\left({}\\right)\\left({}\\right)'.format(other.symbolic, self.symbolic))


	def __imul__(self, other):
		return self.__mul__(other)


	def __div__(self, other):
		other = self._coerce(other)
		return Variable(self.value / other.value, '\\frac{{\\left({}\\right)}}{{\\left({}\\right)}}'.format(self.symbolic, other.symbolic))


	def __rdiv__(self, other):
		other = self._coerce(other)
		return Variable(other.value / self.value, '\\frac{{\\left({}\\right)}}{{\\left({}\\right)}}'.format(other.symbolic, self.symbolic))


	def __idiv__(self, other):
		return self.__div__(other)


	def __pow__(self, other):
		other = self._coerce(other)
		return Variable(self.value ** other.value, '{{\\left({}\\right)}}^{{\\left({}\\right)}}'.format(self.symbolic, other.symbolic))


	def __rpow__(self, other):
		other = self._coerce(other)
		return Variable(other.value ** self.value, '{{\\left({}\\right)}}^{{\\left({}\\right)}}'.format(other.symbolic, self.symbolic))


	def __ipow__(self, other):
		return self.__pow__(other)


	def sum(self, dim=None):
		return Variable(self.value.sum(dim=dim), '\\sum{}{{\\left\\{{{}\\right\\}}}}'.format(('_{{{}}}'.format(dim) if dim is not None else ''), self.symbolic))

	def ln(self):
		return Variable(np.log(self.value), '\\ln{{\\left({}\\right)}}'.format(self.symbolic))

	def get_symbol(self):
		return self.attrs['latex'] + '_{{{}}}'.format(','.join(self.value.dims))

	def equation(self):
		return '{} = {}'.format(self.get_symbol(), self.symbolic)
    
	def display(self):
		if in_ipynb():
			display(Latex('${}$'.format(self.equation())))
		else:
			return '${}$'.format(self.equation())

	def compute(self):
		'''
		right now, this just returns the value
		'''

		return self.value

def get_random_variable(dims):
	data = np.random.random(tuple([len(d[1]) for d in dims]))
	foo = xr.DataArray(data, coords=dims)
	return foo

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
		def do_func(**kwds):
			for kw in kwargs:
				if kw in kwds:
					arg = kwds[kw]
				else:
					if in_ipynb():
						# wigit code
						pass
					else:
						arg = raw_input(kw)
				kwargs[kw] = arg

			func(**kwargs)
		return do_func
	return get_decorator

class ClimateImpactLabDataAPI(object):
	'''
	Implements the interface for Climate Impact Lab users
	'''

	def __init__(self, *args, **kwargs):
		self._populate_random_data()

	def _populate_random_data(self):
		'''
		Provides dummy versions of the variables we need for this demo

		This method represents work that would be done beforehand. The data in 
		these variables should already be prepared in netCDF or csvv files. In 
		the production version, these datasets will also be probabilistic, and 
		climate variables will also be indexed by climate model.
		'''

		dims = {}
		self.database = {}

		with open('database.json', 'r') as fp:
			ds = json.loads(fp.read())

		for dim in ds['dims']:
			dims[dim] = [0]

		for var in ds['variables']:
			self.database[var] = xr.DataArray(np.ones(tuple([1 for d in ds['variables'][var]['dims']])), coords=[(d, dims[d]) for d in ds['variables'][var]['dims']])
			self.database[var].chunk(tuple([1 for d in ds['variables'][var]['dims']]))
			self.database[var].attrs = ds['variables'][var]

	@require('gcp_id','name','latex','description','author','updated')
	def publish(self, gcp_id, name, latex, description, author, updated):

		with open('database.json', 'r') as fp:
			ds = json.loads(fp.read())

		if gcp_id in ds:
			raise KeyError('{} already in dataset'.format(gcp_id))

		ds[gcp_id] = {
			'uuid': hashlib.sha256(np.random.random()).hexdigest(),
			'gcp_id': gcp_id,
			'name': name,
			'latex': latex,
			'description': description,
			'author': author,
			'updated': updated
		}

		with open('database.json', 'w+') as fp:
			fp.write(json.dumps(ds))

	def get_variable(self, varname):
		'''
		The actual API call. 
		'''

		return Variable(self.database[varname])

	def configure(self, *args, **kwargs):
		print('API configuration updated')




