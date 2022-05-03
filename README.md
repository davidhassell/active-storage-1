### Demonstration of possible active storage approach

* The demonstration in this repo uses a modified version of dask:
  https://github.com/davidhassell/dask/tree/active-storage.

* Code changes in the modified dask can be seen at
  https://github.com/davidhassell/dask/pull/1/files.

* The full results of running `demo.py` are shown here, and the dask
  graph visualisations are in the repo.

```console
$ cd active-storage-1
$ python demo.py

Active max(a) = 0.146
Normal max(a) = 0.146

Active mean(a) = 0.046075
Normal mean(a) = 0.046075

Non-active sum(a) = 1.843
    Normal sum(a) = 1.843

Active max(a) + a = [[0.153 0.18  0.149 0.16  0.164 0.183 0.17  0.175]
                     [0.169 0.182 0.191 0.208 0.192 0.219 0.152 0.212]
                     [0.256 0.277 0.27  0.292 0.233 0.249 0.203 0.157]
                     [0.175 0.205 0.185 0.216 0.204 0.218 0.155 0.163]
                     [0.152 0.182 0.165 0.181 0.164 0.183 0.18  0.159]]
Normal max(a) + a = [[0.153 0.18  0.149 0.16  0.164 0.183 0.17  0.175]
                     [0.169 0.182 0.191 0.208 0.192 0.219 0.152 0.212]
                     [0.256 0.277 0.27  0.292 0.233 0.249 0.203 0.157]
                     [0.175 0.205 0.185 0.216 0.204 0.218 0.155 0.163]
                     [0.152 0.182 0.165 0.181 0.164 0.183 0.18  0.159]]

Active sum(max(a) + a) = 7.683
Normal sum(max(a) + a) = 7.683

$ ls -1rt *.png
active_max.png
normal_max.png
non_active_sum.png
normal_sum.png
active_max+a.png
normal_max+a.png
active_sum_max+a.png
normal_sum_max+a.png
```

### Installation

To install the modified version of dask (as given above):

```console
$ # Get the fork code locally with a dir name that won't clash with 'dask/dask'
$ git clone https://github.com/davidhassell/dask.git davids-dask
Cloning into 'davids-dask'...
remote: Enumerating objects: 49976, done.
remote: Total 49976 (delta 0), reused 0 (delta 0), pack-reused 49976
Receiving objects: 100% (49976/49976), 36.87 MiB | 7.48 MiB/s, done.
Resolving deltas: 100% (39227/39227), done.
$ cd davids-dask/
$ # Now check out the required branch from the fork (default branch is 'main')
$ git checkout active-storage
Branch 'active-storage' set up to track remote branch 'active-storage' from 'origin'.
Switched to a new branch 'active-storage'
$ # Do a development install of this so conda uses branch instead of dask/dask
$ pip install -e .
Obtaining file:///home/sadie/davids-dask
...
...
...
  Running setup.py develop for dask
...
Successfully installed dask-2020.12.0+902.geef967a8
```

To check that you are set to use the required `dask` fork branch from the above
steps, you can inspect `conda list`, which should now show something like this:

```console
$ conda list | grep dask
dask                      2020.12.0+902.geef967a8           dev_0    <develop>
```

rather than a result which indicates a standard `dask/dask` install.

`cfdm` is also required to handle the reading of the netCDF file:

```console
$ pip install cfdm
```
