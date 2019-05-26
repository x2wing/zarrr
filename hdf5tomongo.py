from gridfs import GridFS, GridFSBucket
from pymongo import MongoClient
import h5py
import io
import hashlib
import time
import shutil

path = r'D:\GEOSIM PROJECTS\db_del.h5geo'
path = r'D:\GEOSIM PROJECTS\filters2.h5geo'
# path2 = r'D:\GEOSIM PROJECTS\123.txt'
# path = r'D:\GEOSIM PROJECTS\COORD.h5geo'
# path = r'D:\obmen\geosim\geodata\priobka_geo.h5geo'
# ограничение bson
CHUNK_SIZE = 15 * 1024 * 1024
temp_path = 'obj'


def get_md5(path):
    if isinstance(path, bytes):
        _hash = hashlib.md5(path).hexdigest()
    elif isinstance(path, str):
        with open(path, mode='rb') as file:
            _hash = hashlib.md5(file.read()).hexdigest()

    return _hash


def timer(foo):
    """ декоратор. выводит время выполнения методов"""

    def wrapper(self, *args, **kargs):
        tm = time.time()
        result = foo(self, *args, **kargs)
        print(f"ВРЕМЯ ВЫПОЛНЕНИЯ {foo.__name__}", time.time() - tm)
        return result

    return wrapper


# def uploader(h5geo, db):
#     with h5py.File(path, mode='r') as src:
#
#         def printname(name, obj, ):
#             try:
#                 with open('tmp', mode='wb') as tmp:
#                     tmp.copy(obj, name, shallow=True)
#                     # db.upload(name, obj)
#
#             except Exception as error:
#                 print(error)
#                 print(name, obj.name)
#
#         src.visititems(printname)


class DB_GridFS():
    def __init__(self):
        db = MongoClient().GEOSIM_GRIDFS
        self.fs = GridFSBucket(db)

    # @timer
    def upload(self, obj_name, filepath, metadata={'origin_hash': '_hash'}):
        grid_in = self.fs.open_upload_stream(
            obj_name, chunk_size_bytes=CHUNK_SIZE,
            metadata=metadata)
        with open(filepath, mode='rb') as dst:
            grid_in.write(dst)  # grid_in.write(file_like_obj)
        grid_in.close()  # uploaded on close

    # @timer
    def download(self, obj_name, filepath):
        with open(filepath, mode='wb') as dst:
            self.fs.download_to_stream_by_name(obj_name, dst)

    # @timer
    def delete(self, keyname):
        cur = self.fs.find({"filename": keyname})
        for item in cur:
            self.fs.delete(item._id)

class H5Base:
    def __init__(self, db, src_path):
        self.db = db
        self.h5geo_path = src_path


class H5Upload(H5Base):
    def __init__(self, db, src_path):
        super().__init__(db, src_path)



    def cut(self, name, obj, ):
        """метод для visititem"""

        with h5py.File(temp_path, mode='w') as dst:

            try:
                if isinstance(obj, h5py.Group):
                    group = dst.create_group(name)
                    for key, value in obj.attrs.items():
                        # print(key, value)
                        group.attrs[key] = value

                elif isinstance(obj, h5py.Dataset):
                    dst.copy(obj, name)

            except Exception as error:
                print(error)
                print(name, obj.name)

        self.send(name, temp_path, metadata=None)



    def send(self, name, temp_path, metadata):
        self.db.upload(name, temp_path, metadata)


    def upload(self):
        with h5py.File(self.h5geo_path, mode='r') as src:
            src.visititems(self.cut)


class H5Download(H5Base):
    def __init__(self, db, dst_path):
        super().__init__(db, dst_path)

    def download(self):
        for n, item in enumerate(self.db.fs.find()):
            print(n, item.filename)


            self.db.download(item.filename, temp_path)
            with h5py.File(temp_path, mode='r') as tmp, h5py.File(self.h5geo_path, mode='a') as dst:
                dst.copy(tmp[item.filename], item.filename)



    def test(self, condition):
        for n, item in enumerate(self.db.fs.find(condition).limit(5)):
            print(n, item.filename)




if __name__ == '__main__':
    # upload(path)
    # download(path, 'filters2.h5geo')
    # get_by_hash()
    name_key = 'COORD_big'
    db = DB_GridFS()
    # uploader = H5Upload(db, path)
    # uploader.upload()
    downloader = H5Download(db, 'result.h5geo')
    downloader.download()


    # fd = open(path, mode='rb')
    # db.delete('COORD')
    # db.upload(name, path)
    # db.download(name, path2)
    # db.delete(name)

    # db.download_uncompress(name, path2)

