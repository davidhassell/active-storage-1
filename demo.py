from copy import deepcopy
from functools import reduce
from math import prod
from operator import mul
import numpy as np
import netCDF4
import dask.array as da

    
class NetCDFArray:
    """An array stored in a netCDF file.

    Supports active storage operations.

    This object has been simplified from cfdm.NetCDFArray (on which it
    is based) in order to make the code easier to understand. E.g. it
    doesn't support netCDF groups and doesn't properly deal with
    string data types. This functionality would all be reinstated in
    an operational version.

    """

    def __init__(self, filename=None, ncvar=None, dtype=None, shape=None):
        self.filename = filename
        self.ncvar = ncvar
        self.dtype = dtype
        self.shape = shape
        self.ndim = len(shape)
        self.size = prod(shape)
        
    def __getitem__(self, indices):
        # Read the data. Either a normal read by the local client, in
        # which case 'data' will be a numpy array; or an "active
        # storage read + operation", in which case I'm assuming for
        # now that 'data' will be a dictionary.
        data = self.read(indices)
        
        if self.active_storage_op:
            # Active storage is in play, so aggregate the reduced
            # values for each file-chunk part that overlaps this
            # requested slice, and return a value that is expected by
            # the 'aggregate' callable argument of
            # dask.array.reduction (which is typically an array or a
            # dictionary).
            data = self.aggregate_active_reductions(data)

        # Return the either the numpy array from the normal read, or
        # the dictionary containing reductions calculated by the
        # active storage.
        return data

    def __repr__(self):
        return f"<{self.__class__.__name__}{self.shape}: {self}>"

    def __str__(self):
        return f"file={self.filename} {self.ncvar}"

    def _active_chunk_functions(self):
        return {
            "min": self.active_min,
            "max": self.active_max,
            "mean": self.active_mean,
        }

    @property
    def active_storage_op(self):
        return getattr(self, "_active_storage_op", None)

    @active_storage_op.setter
    def active_storage_op(self, value):
        self._active_storage_op = value

    @property
    def op_axis(self):
        return getattr(self, "_op_axis", None)

    @op_axis.setter
    def op_axis(self, value):
        self._op_axis = value

    @staticmethod
    def active_min(a, **kwargs):
        """Chunk calculations for the minimum.

        Assumes that the calculations have already been done,
        i.e. that *a* is already the minimum.

        This function is intended to be passed in to
        `dask.array.reduction()` as the ``chunk`` parameter. Its
        return signature must be the same as the non-active chunks
        function that it is replacing.

        :Parameters:

            a: `dict`

        :Returns:

            `numpy.ndarray`
                Currently set up to replace `dask.array.chunk.min`.

        """
        return a['min']

    @staticmethod
    def active_max(a, **kwargs):
        """Chunk calculations for the maximum.

        Assumes that the calculations have already been done,
        i.e. that *a* is already the maximum.

        This function is intended to be passed in to
        `dask.array.reduction()` as the ``chunk`` parameter. Its
        return signature must be consistent with that expected by the
        functions of the ``aggregate`` and ``combine`` parameters.

        :Parameters:

            a: `dict`

        :Returns:

            `numpy.ndarray`
                Currently set up to replace `dask.array.chunk.max`.

        """
        return a['max']

    @staticmethod
    def active_mean(a, **kwargs):
        """Chunk calculations for the mean.

        Assumes that the calculations have already been done,
        i.e. that *a* is already the mean.

        This function is intended to be passed in to
        `dask.array.reduction()` as the ``chunk`` parameter. Its
        return signature must be the same as the non-active chunks
        function that it is replacing.

        :Parameters:

            a: `dict`

        :Returns:

            `dict`
                Currently set up to replace
                `dask.array.reductions.mean_chunk`

        """
        return {"n": a['V1'], "total": a['sum']}
                    
    def aggregate_active_reductions(self, data):
        """Active storage is in play, so aggregate the reduced values.

        Each reduced value corresponds to a partial value calcuated
        from a different disk-chunk.

        :Returns:

            `dict`
                 The output in a form expected by the various
                 `active_*` methods.

        """
        active_storage_op = self.active_storage_op
        if active_storage_op == "max":
            data = {'max': data['max'].max()}
        elif active_storage_op == "min":
            data = {'min': data['min'].min()}
        elif active_storage_op == "mean":
            data = {'V1': data['V1'].sum(),
                    'sum': data['sum'].sum()}
        else:
            raise ValueError(
                f"Unkown active storage operation {active_storage_op!r}"
                f"requested on file {self.filename!r} variable {self.ncvar!r}"
            )
        
        # It is *essential* that the returned data has the same number
        # of dimensions as the full array. Otherwise
        # `dask.array.reduction` will not work.
        #
        # Note: We are assuming for now (because it's simpler) that
        #       all axes are being reduced. The following lines will
        #       need replacing when (if) we support reductions along a
        #       subset of the axes.
        ndim = self.ndim
        data = {key, d.reshape((1,) * ndim) for key, d in data.items()}

        # Return the partial reductions
        return data
    
    def set_active_storage_op(self, op, axis=None):
        if op not in self._active_chunk_functions():
            raise ValueError(f"Invalid active storage operation: {op!r}")

        a = self.copy()
        a.active_storage_op = op
        a.op_axis = axis
        return a

    def get_active_chunk_function(self):
        try:
            return self._active_chunk_functions()[self.active_storage_op]
        except KeyError:
            raise ValueError("no active storage operation has been set")

    def copy(self):
        return deepcopy(self)

    def read(self, indices):
        """"""
#        if self.active_storage_op:
        if False: ## ! Set to False for testing
            # Active storage read. Returns a dictionary.
            data = Active(self.filename, self.dtype, self._FillValue,
                          self.missing_value, self.active_storage_op,
                          indices)
            
            data = data.get()
        else:
            # Normal read by local client. Returns a numpy array.
            #
            # In production code groups, masks, string types,
            # etc. will need to be accounted for here.
            try:
                nc = netCDF4.Dataset(self.filename, "r")
            except RuntimeError as error:
                raise RuntimeError(f"{error}: {self.filename}")

            data = nc.variables[self.ncvar][indices]
            nc.close()
        
        return data
        

if __name__ == "__main__":
    import os

    import cfdm
    import dask

    # Check that we're using the modified dask
    try:
        dask.array.reductions.actify
    except AttributeError:
        raise AttributeError(
            "No 'dask.array.reductions.actify' function.\n"
            f"dask path: {dask.__file__}\n"
            f"PYTHONPATH={os.environ.get('PYTHONPATH', '')}"
        )

    # Lazy read of the netCDF file (using the same API as used by
    # cf-python)
    f = NetCDFArray(filename="file.nc", ncvar="q", shape=(5, 8), dtype=np.dtype(float))

    # In-memory version of 'f'
    x = cfdm.read("file.nc")[0].data.array

    df = da.from_array(f, chunks=(3, 4))
    dx = da.from_array(x, chunks=df.chunks)

    g = da.max(df)
    y = da.max(dx)
    print("\nActive max(a) =", g.compute())
    print("Normal max(a) =", y.compute())
    g.visualize(filename="active_max.png")
    y.visualize(filename="normal_max.png")

    g = da.mean(df)
    y = da.mean(dx)
    print("\nActive mean(a) =", g.compute())
    print("Normal mean(a) =", y.compute())
    g.visualize(filename="active_mean.png")
    y.visualize(filename="normal_mean.png")

    # da.sum has been "actified", but NetCDFArray does not support
    # "sum" as an active operation
    g = da.sum(df)
    y = da.sum(dx)
    print("\nNon-active sum(a) =", g.compute())
    print("    Normal sum(a) =", y.compute())
    g.visualize(filename="non_active_sum.png")
    y.visualize(filename="normal_sum.png")

    g = da.max(df) + df
    y = da.max(dx) + dx
    print("\nActive max(a) + a =", g.compute())
    print("Normal max(a) + a =", y.compute())
    g.visualize(filename="active_max+a.png")
    y.visualize(filename="normal_max+a.png")

    g = da.sum(da.max(df) + df)
    y = da.sum(da.max(dx) + dx)
    print("\nActive sum(max(a) + a) =", g.compute())
    print("Normal sum(max(a) + a) =", y.compute())
    g.visualize(filename="active_sum_max+a.png")
    y.visualize(filename="normal_sum_max+a.png")


# Original version    
#class NetCDFArray0:
#    """An array stored in a netCDF file.
#
#    Supports active storage operations.
#
#    This object has been simplified from cfdm.NetCDFArray (o which it
#    is based) in order to make the code easier to understand. E.g. it
#    doesn't support netCDF groups and doesn't properly deal with
#    string data types.
#
#    """
#
#    def __init__(self, filename=None, ncvar=None, dtype=None, shape=None):
#        self.filename = filename
#        self.ncvar = ncvar
#        self.dtype = dtype
#        self.shape = shape
#        self.ndim = len(shape)
#        self.size = prod(shape)
#
#    def __getitem__(self, indices):
#        try:
#            netcdf = netCDF4.Dataset(self.filename, "r")
#        except RuntimeError as error:
#            raise RuntimeError(f"{error}: {self.filename}")
#
#        array = netcdf.variables[self.ncvar][indices]
#        # This where active storage would really happen
#
#        netcdf.close()
#
#        # Spoof active storage
#        if self.active_storage_op == "max":
#            array = np.max(array, axis=self.op_axis, keepdims=True)
#
#        if self.active_storage_op == "min":
#            array = np.min(array, axis=self.op_axis, keepdims=True)
#
#        if self.active_storage_op == "mean":
#            array = (
#                np.full((1,) * array.ndim, array.size),
#                np.sum(array, axis=self.op_axis, keepdims=True),
#            )
#
#        return array
#
#    def __repr__(self):
#        return f"<{self.__class__.__name__}{self.shape}: {self}>"
#
#    def __str__(self):
#        return f"file={self.filename} {self.ncvar}"
#
#    def _active_chunk_functions(self):
#        return {
#            "min": self.active_min,
#            "max": self.active_max,
#            "mean": self.active_mean,
#        }
#
#    @property
#    def active_storage_op(self):
#        return getattr(self, "_active_storage_op", None)
#
#    @active_storage_op.setter
#    def active_storage_op(self, value):
#        self._active_storage_op = value
#
#    @property
#    def op_axis(self):
#        return getattr(self, "_op_axis", None)
#
#    @op_axis.setter
#    def op_axis(self, value):
#        self._op_axis = value
#
#    @staticmethod
#    def active_min(a, **kwargs):
#        return a
#
#    @staticmethod
#    def active_max(a, **kwargs):
#        return a
#
#    @staticmethod
#    def active_mean(a, **kwargs):
#        return {"n": a[0], "total": a[1]}
#
#    def set_active_storage_op(self, op, axis=None):
#        if op not in self._active_chunk_functions():
#            raise ValueError(f"Invalid active storage operation: {op!r}")
#
#        a = self.copy()
#        a.active_storage_op = op
#        a.op_axis = axis
#        return a
#
#    def get_active_chunk_function(self):
#        try:
#            return self._active_chunk_functions()[self.active_storage_op]
#        except KeyError:
#            raise ValueError("no active storage operation has been set")
#
#    def copy(self):
#        return deepcopy(self)

