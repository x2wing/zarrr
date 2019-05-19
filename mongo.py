from  zarr import MongoDBStore
import pymongo
import h5py
import zarr
import sys
import zarr.storage
from numcodecs import Zstd, Blosc
compressor = Blosc(cname='zstd', clevel=3, shuffle=Blosc.BITSHUFFLE)


h5_file = r'D:\pylerning\BashNIPIneft-dev\HDF_FILES\DSETBIG.h5geo'
h5_dst = 'DSET0'
def write():
    with h5py.File(h5_file) as source:


        store = zarr.MongoDBStore(database='geosim',  collection='Приобка')
        # z = zarr.zeros((10, 10), chunks=(5, 5), store=store, overwrite=True)
        dest = zarr.open(store, mode='w', )
        # print('compressor', dest.compressor)
        zarr.storage.default_compressor = Zstd(level=1)
        # zarr.storage.compressor =Zstd(level=1)
        zarr.copy_all(source, dest, log=sys.stdout)
        # z[...] = 42
        print(dest['DSET0'])
        store.close()

def read():
    store = zarr.MongoDBStore(database='geosim', collection='Приобка')
    source = zarr.open(store, mode='r')
    # source = z['DSET0']
    with h5py.File(h5_dst) as dest:
        zarr.copy_all(source, dest, log=sys.stdout, if_exists='replace')

    # print(z['DSET0'][:])


if __name__ == '__main__':
    write()
    read()