from  zarr import MongoDBStore
import pymongo



import zarr
store = zarr.MongoDBStore(database='geosim',  collection='Приобка')
# z = zarr.zeros((10, 10), chunks=(5, 5), store=store, overwrite=True)
z = zarr.open(store, mode='r')
# z[...] = 42
print(z[:])
store.close()