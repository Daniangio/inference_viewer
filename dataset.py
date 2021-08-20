import multiprocessing
import numpy as np
import cv2
import torch
from kafka import KafkaConsumer
from torch.utils.data import IterableDataset


def equalize(image):
    if len(image.shape) == 2:
        hist_equalized_image = cv2.equalizeHist(image)
    else:
        hist_equalized_image = cv2.cvtColor(image, cv2.COLOR_BGR2YCrCb)
        hist_equalized_image[:, :, 0] = cv2.equalizeHist(hist_equalized_image[:, :, 0])
        hist_equalized_image = cv2.cvtColor(hist_equalized_image, cv2.COLOR_YCrCb2BGR)
    return hist_equalized_image


def normalize_channels(image):
    _, _, channels = image.shape
    for c in range(channels):
        image[:, :, c] -= np.min(image[:, :, c])
    return image


def double_area_side(y, x, h, w):
    h_half_side = h // 2
    y -= h_half_side
    h += h_half_side * 2

    w_half_side = w // 2
    x -= w_half_side
    w += w_half_side * 2
    return y, x, h, w


def crop_image(image, y, x, h, w):
    y_from, x_from = y, x
    y_to, x_to = y + h, x + w
    y_offset, x_offset = 0, 0
    if y_from < 0:
        y_offset = -y_from
        y_from = 0
    if x_from < 0:
        x_offset = -x_from
        x_from = 0

    cropped_image = np.zeros((h + y_offset, w + x_offset, image.shape[-1]))
    crop = image[y_from:y_to, x_from:x_to, :]
    cropped_image[y_offset:y_offset + crop.shape[0], x_offset:x_offset + crop.shape[1], :] = crop
    return cropped_image


def crop_on_multiple_sizes(complete_image, y_from, y_to, x_from, x_to):
    h_orig, w_orig = y_to - y_from, x_to - x_from
    near_image = cv2.resize(crop_image(complete_image, y_from, x_from, h_orig, w_orig), (h_orig, w_orig),
                            interpolation=cv2.INTER_AREA)
    y_from, x_from, h, w = double_area_side(y_from, x_from, h_orig, w_orig)
    mid_image = cv2.resize(crop_image(complete_image, y_from, x_from, h, w), (h_orig, w_orig),
                           interpolation=cv2.INTER_AREA)
    return np.concatenate((near_image, mid_image), axis=2).astype(np.float32)


class ImageItem:
    def __init__(self, data, orig_data, code, code_index, stripe_index, filename):
        self.data = data
        self.orig_data = orig_data
        self.code = code
        self.code_index = code_index
        self.stripe_index = stripe_index
        self.filename = filename.split('\\')[-1]


class KafkaIterableDataset(IterableDataset):
    def __init__(self, kafka_main_topic: str, input_size: int):
        super(KafkaIterableDataset).__init__()
        self.orchestration_topic = kafka_main_topic
        self.input_size = input_size
        self.client_id = str(0)
        # In the same process (and across processes as well), all threads belong to the same topic group.
        # This way, they split the workload within the same topic (if kafka topic has enough partitions)
        self.topic_group_id = str(0)
        self.consumer = None
        self.worker_id = 0

    def __del__(self):
        print(f'{self.worker_id} - exiting')
        if self.consumer:
            self.consumer.close()

    def __getitem__(self, index):
        pass

    def __iter__(self):
        def get_topic_consumer(topic, client_id, group_id):
            return KafkaConsumer(topic,
                                 bootstrap_servers=['localhost:29092'],
                                 client_id=client_id,
                                 group_id=group_id,
                                 auto_offset_reset='latest'
                                 )

        if self.consumer is None:
            self.consumer = get_topic_consumer(self.orchestration_topic, self.client_id, self.topic_group_id)
            # print(f'{self.worker_id} - start subscription to topic {self.orchestration_topic} - {self.consumer}')
        return self

    def __next__(self):
        while True:
            retries = 5
            while self.consumer is not None:
                raw_messages = self.consumer.poll(timeout_ms=200, max_records=1)
                if len(raw_messages) == 0:
                    if retries > 0:
                        # print(f'{self.worker_id} - retries {retries}')
                        retries -= 1
                    else:
                        # print(f'{self.worker_id} - stop iteration')
                        self.consumer.close()
                        self.consumer = None
                        raise StopIteration
                else:
                    try:
                        for topic_partition, messages in raw_messages.items():
                            message = messages[0].value.decode('utf-8')
                            _, filename, code, code_index, stripe_index = message.split('|')
                            print(f'{self.worker_id} - new stripe loaded in memory: {filename}')
                            orig_image = cv2.imread(filename, cv2.IMREAD_COLOR)
                            if orig_image is None:
                                raise FileNotFoundError(f'file {filename} not found')
                            stripe_image = equalize(orig_image)
                            stripe_image = cv2.cvtColor(stripe_image, cv2.COLOR_BGR2HSV)
                            stripe_image = normalize_channels(stripe_image)

                            # Tiling stripe_image
                            chunk_x, chunk_y = self.input_size, self.input_size
                            height, width, dim = 1024, 1024, 3  # stripe_image.shape
                            images, orig_images = [], []
                            for y in range(0, height, chunk_y):
                                for x in range(0, width, chunk_x):
                                    y_from, y_to = y, y + chunk_y
                                    x_from, x_to = x, x + chunk_x
                                    if y_to > height:
                                        y_from, y_to = height - chunk_y, height
                                    if x_to > width:
                                        x_from, x_to = width - chunk_x, width
                                    image = crop_on_multiple_sizes(stripe_image, y_from, y_to, x_from, x_to)
                                    images.append(image)
                                    orig_images.append(orig_image[y_from:y_to, x_from:x_to, :])
                            data = (np.stack(images, axis=0) / 255).astype('float32').transpose((0, 3, 1, 2))
                            orig_data = (np.stack(orig_images, axis=0) / 255).astype('float32').transpose((0, 3, 1, 2))
                            item = ImageItem(data, orig_data, code, code_index, stripe_index, filename)
                            return vars(item)
                    except FileNotFoundError as e:
                        print(str(e))


def worker_init_fn(worker_id):
    worker_info = torch.utils.data.get_worker_info()
    dataset = worker_info.dataset  # the dataset copy in this worker process
    dataset.worker_id = worker_id
    dataset.client_id = str(worker_id)
