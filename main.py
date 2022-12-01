import fnmatch

import shutil

import time
import yaml
import logging
import os

from cytomine import Cytomine
from cytomine.models import StorageCollection, Project

logging.basicConfig()
logger = logging.getLogger("cytomine.client")
logger.setLevel(logging.INFO)

_configs = {}


def readconfig():
    with open(os.getcwd() + '/config.txt') as f:
        try:
            data = yaml.load(f, Loader=yaml.FullLoader)
            return data
        except Exception as e:
            logger.error(e)
            logger.error("config.txt file format error")
            exit(1)


def get_one_image(imagetype):
    for dirPath, dirNames, filenames in os.walk(_configs["upload_filepath"]):
        if len(os.listdir(dirPath)) == 0 and len(dirPath) > len(_configs["upload_filepath"]):
            os.rmdir(dirPath)
        for fileName in filenames:
            if fnmatch.fnmatch(fileName, '*.' + imagetype):
                return dirPath, fileName
    return None, None


def get_image_types(image_formats):
    format_list = image_formats.split(',')
    return format_list


def is_file_changing(dir_path, file_name):
    now_size = os.path.getsize(dir_path + '/' + file_name)
    time.sleep(1)
    latest_size = os.path.getsize(dir_path + '/' + file_name)
    if now_size == latest_size:
        return False
    else:
        return True


def upload_image(dir_path, file_name, storage):
    with Cytomine(host=_configs["host"], public_key=_configs["admin_public_key"],
                  private_key=_configs["admin_private_key"]) as cytomine:

        uploaded_file = cytomine.upload_image(upload_host=_configs["upload_host"],
                                              filename=dir_path + "/" + file_name,
                                              id_storage=storage.id,
                                              id_project=_configs["id_project"])

        if uploaded_file:
            logger.info(uploaded_file)
            return True
        else:
            return False


def move_file(dir_path, file_name, upload_filepath,uploaded_filepath):
    now_filepath = dir_path + '/' + file_name
    new_filepath = uploaded_filepath + '/' + dir_path.replace(upload_filepath, '') + '/'
    if not os.path.exists(new_filepath):
        os.makedirs(new_filepath)
    if os.path.exists(new_filepath + file_name):
        os.remove(new_filepath + file_name)
    shutil.move(now_filepath, new_filepath)


if __name__ == '__main__':
    _configs = readconfig()

    # Check inputs
    if not os.path.exists(_configs["upload_filepath"]):
        logger.error(_configs["upload_filepath"] + " does not exist, please check config.txt")
        exit(1)
    if not os.path.exists(_configs["uploaded_filepath"]):
        logger.error(_configs["uploaded_filepath"] + " does not exist, please check config.txt")
        exit(1)
    logger.info("image path: {}".format(_configs["upload_filepath"]))
    image_types = get_image_types(_configs["image_types"])
    logger.info("image types: {}".format(image_types))

    with Cytomine(host=_configs["host"], public_key=_configs["admin_public_key"],
                  private_key=_configs["admin_private_key"]) as cytomine:
        # Check that the given project exists
        if _configs["id_project"]:
            project = Project().fetch(_configs["id_project"])
            if not project:
                logger.info("project is not exist")
                exit(1)

        # To upload the image, we need to know the ID of your Cytomine storage.
        storages = StorageCollection().fetch()
        my_storage = next(filter(lambda storage: storage.user == cytomine.current_user.id, storages))
        if not my_storage:
            logger.info("storage is not exist")
            exit(1)
    while True:
        for imgtype in image_types:
            dirpath, filename = get_one_image(imgtype)
            if filename is None:
                time.sleep(2)
                continue
            logger.info("  uploading {}{} ...".format(dirpath, filename))

            status = is_file_changing(dirpath, filename)
            if status:
                logger.info("wait for file {} updating ...".format(filename))
                time.sleep(1)
                continue
            if upload_image(dirpath, filename, my_storage):
                move_file(dirpath, filename , _configs["upload_filepath"], _configs["uploaded_filepath"])

