from copy import deepcopy
from functools import reduce
from operator import mul
import numpy as np
import netCDF4
import dask.array as da


class NetCDFArray:
    """An array stored in a netCDF file.

    Supports active storage operations.

    This object has been simplified from cfdm.NetCDFArray (o which it
    is based) in order to make the code easier to understand. E.g. it
    doesn't support netCDF groups and doesn't properly deal with
    string data types.

    """

    def __init__(self, filename=None, ncvar=None, dtype=None, shape=None):
        self.filename = filename
        self.ncvar = ncvar
        self.dtype = dtype
        self.shape = shape
        self.ndim = len(shape)
        self.size = reduce(mul, shape, 1)

    def __getitem__(self, indices):
        try:
            netcdf = netCDF4.Dataset(self.filename, "r")
        except RuntimeError as error:
            raise RuntimeError(f"{error}: {self.filename}")

        array = netcdf.variables[self.ncvar][indices]
        # This where active storage would really happen

        netcdf.close()

        # Spoof active storage
        if self.active_storage_op == "max":
            array = np.max(array, axis=self.op_axis, keepdims=True)

        if self.active_storage_op == "min":
            array = np.min(array, axis=self.op_axis, keepdims=True)

        if self.active_storage_op == "mean":
            array = (
                np.full((1,) * array.ndim, array.size),
                np.sum(array, axis=self.op_axis, keepdims=True),
            )

        return array

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
        return a

    @staticmethod
    def active_max(a, **kwargs):
        return a

    @staticmethod
    def active_mean(a, **kwargs):
        return {"n": a[0], "total": a[1]}

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