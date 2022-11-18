import fnmatch
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
    for dirPath, dirNames, filenames in os.walk(_configs["filepath"]):
        for fileName in filenames:
            if fnmatch.fnmatch(fileName, '*.' + imagetype):
                if not fnmatch.filter(filenames, fileName + ".uploaded"):
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


def create_uploaded_tag(dir_path, file_name):
    source_filepath = dir_path + '/' + file_name
    uploaded_prove_filepath = source_filepath + ".uploaded"
    open(uploaded_prove_filepath, "w").close()


if __name__ == '__main__':
    _configs = readconfig()

    # Check inputs
    if not os.path.exists(_configs["filepath"]):
        logger.error(_configs["filepath"] + " does not exist, please check config.txt")
        exit(1)
    logger.info("image path: {}".format(_configs["filepath"]))
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

    for imgtype in image_types:
        while True:
            dirpath, filename = get_one_image(imgtype)
            if filename is None:
                break
            logger.info("  uploading {}{} ...".format(dirpath, filename))

            status = is_file_changing(dirpath, filename)
            if status:
                logger.info("wait for file {} updating ...".format(filename))
                time.sleep(1)
                continue

            if upload_image(dirpath, filename, my_storage):
                create_uploaded_tag(dirpath, filename)

    logger.info("all images are uploaded!!!")

