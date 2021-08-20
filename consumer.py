import os

import cv2
import torch
import torchvision
from kafka import KafkaAdminClient
from kafka.admin import NewTopic
from kafka.errors import KafkaError
from torch.utils.data import DataLoader
import numpy as np

from config import config
from dataset import KafkaIterableDataset, worker_init_fn
from models.segmentator import Segmentator


def create_topic(admin_client, topic: str, partitions: int):
    try:
        topic_list = [NewTopic(name=topic, num_partitions=partitions, replication_factor=1)]
        admin_client.create_topics(new_topics=topic_list, validate_only=False)
        print(f'topic {topic} created!')
    except KafkaError as ke:
        print(str(ke))
        pass


def main():
    admin_client = KafkaAdminClient(
        bootstrap_servers="localhost:29092",
        client_id='consumer'
    )

    create_topic(admin_client=admin_client, topic=config['kafka_main_topic'],
                 partitions=config['model_data_loader_workers'])

    device = config["device"]
    net = Segmentator(input_size=512, in_channels=3 * 2, out_channels=1)

    weights = os.path.join(config['model_weights_folder'], net.get_weights_filename())
    if os.path.isfile(weights):
        print(f'Loading model weights for net from {weights}')
        try:
            net.load_state_dict(torch.load(weights, map_location='cpu'), strict=False)
        except Exception as e:
            print(str(e))

    net = net.to(device)
    net.train()
    ds = KafkaIterableDataset(kafka_main_topic=config['kafka_main_topic'], input_size=net.input_size)
    dl = DataLoader(ds, num_workers=config['model_data_loader_workers'], batch_size=1, worker_init_fn=worker_init_fn)
    while True:
        try:
            with torch.no_grad():
                for item_batch in dl:
                    data = item_batch['data'].squeeze()
                    orig_data = item_batch['orig_data'].squeeze()
                    data = data.to(device)
                    output = net(data)
                    images_grid = torchvision.utils.make_grid(orig_data.detach().cpu(),
                                                              nrow=int(np.sqrt(orig_data.size(0)))).permute(1, 2,
                                                                                                            0).numpy()
                    images_grid_near = images_grid[:, :, :3]
                    masks_grid = torchvision.utils.make_grid(output.detach().cpu(),
                                                             nrow=int(np.sqrt(output.size(0)))).permute(1, 2, 0).numpy()
                    images_grid_near[masks_grid > 0.5] = (images_grid_near[masks_grid > 0.5] + 1) / 2

                    save_folder = os.path.join(config['output_folder'], item_batch["code"][0])
                    if not os.path.exists(save_folder):
                        os.makedirs(save_folder)
                    save_path = os.path.join(save_folder,
                                             f'{item_batch["stripe_index"][0]}_{item_batch["filename"][0]}')
                    cv2.imwrite(save_path, 255 * images_grid_near)

                    cv2.imshow('win', images_grid_near)
                    cv2.waitKey(1000)
        except Exception as e:
            print(str(e))
        except:
            print('\nExiting')
            exit(0)


if __name__ == '__main__':
    main()
