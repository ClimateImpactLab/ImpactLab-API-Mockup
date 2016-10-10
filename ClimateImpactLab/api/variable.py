
    
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

    derivation : str (optional)
        Latex representation. For this example, assume we can pull 'symbol' 
        from attrs or use string representation of data.

    For math operations, this class uses the underlying 'value' attributes to do 
    the computation. Therefore, if we configure the objects to use dask rather 
    than in-memory, and figure out how to do the dask distributed computing, 
    this configuration should work.

    '''
    def __init__(self, api, value, derivation=None, derived=True, dependencies=[]):
        self.api = api
        self.value=value
        self.derived = derived
        self.dependencies = set(dependencies)
        self._derivation = derivation

    def __repr__(self):
        return self.value.__repr__()

    def _coerce(self, value):
        if not isinstance(value, Variable):
            return Variable(api=self.api, value=value, derived=False)
        return value

    @property
    def attrs(self):
        return self.value.attrs

    @attrs.setter
    def attrs(self, value):
        self.value.attrs = value

    @property
    def latex(self):
        if hasattr(self.value, 'attrs') and 'latex' in self.attrs:
            return self.attrs['latex'] + self.get_latex_dims()
        return str(self.value)

    @latex.setter
    def latex(self, value):
        raise ValueError('Cannot assign to latex. Try changing symbol.')

    @property
    def derivation(self):
        if self._derivation is None:
            return self.latex
        return self._derivation

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
    def latest(self):
        try:
            return sorted([v for k, v in self.attrs['versions'].items()], key=lambda x: x[0])[0]
        except (KeyError, ValueError, AttributeError):
            return None

    @latest.setter
    def latest(self, value):
        raise ValueError('Cannot set latest')

    def _get_dependencies(self, other=None):
        dependencies = self.dependencies
        if other:
            dependencies |= other.dependencies
        if self.latest:
            dependencies |= set([self.latest])
        if other and other.latest:
            dependencies |= set([other.latest])
        return dependencies

    def __add__(self, other):
        other = self._coerce(other)

        return Variable(
            api=self.api, 
            value=self.value + other.value, 
            derivation ='{} + {}'.format(self.derivation, other.derivation), 
            dependencies = self._get_dependencies(other))


    def __radd__(self, other):
        other = self._coerce(other)
        return Variable(
            api=self.api, 
            value=other.value + self.value, 
            derivation ='{} + {}'.format(other.derivation, self.derivation), 
            dependencies = self._get_dependencies(other))


    def __iadd__(self, other):
        return self.__add__(other)


    def __sub__(self, other):
        other = self._coerce(other)
        return Variable(
            api=self.api, 
            value=self.value - other.value, 
            derivation ='{} - {}'.format(self.derivation, other.derivation), 
            dependencies = self._get_dependencies(other))


    def __rsub__(self, other):
        other = self._coerce(other)
        return Variable(
            api=self.api, 
            value=other.value - self.value, 
            derivation ='{} - {}'.format(other.derivation, self.derivation), 
            dependencies = self._get_dependencies(other))


    def __isub__(self, other):
        return self.__sub__(other)


    def __mul__(self, other):
        other = self._coerce(other)
        return Variable(
            api=self.api, 
            value=self.value * other.value, 
            derivation ='\\left({}\\right)\\left({}\\right)'.format(self.derivation, other.derivation), 
            dependencies = self._get_dependencies(other))


    def __rmul__(self, other):
        other = self._coerce(other)
        return Variable(
            api=self.api, 
            value=other.value * self.value, 
            derivation ='\\left({}\\right)\\left({}\\right)'.format(other.derivation, self.derivation), 
            dependencies = self._get_dependencies(other))


    def __imul__(self, other):
        return self.__mul__(other)


    def __div__(self, other):
        other = self._coerce(other)
        return Variable(
            api=self.api, 
            value=self.value / other.value, 
            derivation ='\\frac{{\\left({}\\right)}}{{\\left({}\\right)}}'.format(self.derivation, other.derivation), 
            dependencies = self._get_dependencies(other))


    def __rdiv__(self, other):
        other = self._coerce(other)
        return Variable(
            api=self.api, 
            value=other.value / self.value, 
            derivation ='\\frac{{\\left({}\\right)}}{{\\left({}\\right)}}'.format(other.derivation, self.derivation), 
            dependencies = self._get_dependencies(other))


    def __idiv__(self, other):
        return self.__div__(other)


    def __pow__(self, other):
        other = self._coerce(other)
        return Variable(
            api=self.api, 
            value=self.value ** other.value, 
            derivation ='{{\\left({}\\right)}}^{{\\left({}\\right)}}'.format(self.derivation, other.derivation), 
            dependencies = self._get_dependencies(other))


    def __rpow__(self, other):
        other = self._coerce(other)
        return Variable(
            api=self.api, 
            value=other.value ** self.value, 
            derivation ='{{\\left({}\\right)}}^{{\\left({}\\right)}}'.format(other.derivation, self.derivation), 
            dependencies = self._get_dependencies(other))


    def __ipow__(self, other):
        return self.__pow__(other)


    def sum(self, dim=None):
        return Variable(
            api=self.api, 
            value=self.value.sum(dim=dim),
            derivation = '\\sum{}{{\\left\\{{{}\\right\\}}}}'.format(('_{{{}\in {}}}'.format(dim, dim.upper()) if dim is not None else ''), self.derivation), 
            dependencies = self._get_dependencies())

    def ln(self):
        return Variable(
            api=self.api, 
            value=np.log(self.value), 
            derivation ='\\ln{{\\left({}\\right)}}'.format(self.derivation), 
            dependencies = self._get_dependencies())

    def get_latex_dims(self):
        return '_{{{}}}'.format(','.join(map(lambda d: self.api.dims[d]['latex'], self.value.dims)))

    def equation(self):
        try:
            symbol = self.symbol + self.get_latex_dims() + ' = '
        except:
            symbol = ''
        return '{}{}'.format(symbol, self.derivation)
    
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



class _VariableGetter(object):
    def __init__(self, var=None):
        if var:
            self._var = var
            self.get_var = self._get_var
    def _get_var(self):
        return self._var
    def __repr__(self):
        return '<{}.{} object : {}>'.format(self.__class__.__module__, self.__class__.__name__, self.list_variables())

    def list_variables(self):
        return sorted(self.__dict__.keys())
