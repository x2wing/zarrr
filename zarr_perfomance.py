import zarr
from numcodecs import Zstd, Blosc
import numpy as np
# store = zarr.MongoDBStore(database='geosim',  collection='Приобка')
store = zarr.DirectoryStore('array.zarr')
# z = zarr.zeros((10, 10), chunks=(5, 5), store=store, overwrite=True)
dest = zarr.open(store, mode='w', )
# print('compressor', dest.compressor)
# zarr.storage.default_compressor = Zstd(level=1)
# dest.create_dataset("tst", shape=(10000,10000,10000))
#
for i in range(50):
    z = dest.create_dataset(f"tdf{i}",shape=(1000000, 1000), chunks=(1000000/2, 1000/2), dtype=np.float)
    z[:] = np.arange(1000000*1000).reshape((1000000,1000))
    # print(z, z[:])
# for a in dest.values():
#     print(a,b)