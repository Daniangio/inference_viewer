import segmentation_models_pytorch as smp
from torch import nn

from config import config


class Segmentator(nn.Module):
    def __init__(self, input_size=512, in_channels=3, out_channels=1, **_):
        super(Segmentator, self).__init__()
        self.input_size = input_size
        self.model = smp.DeepLabV3Plus(
            in_channels=in_channels,
            classes=out_channels,
            encoder_name='se_resnext50_32x4d',
            encoder_weights='imagenet',
            activation='sigmoid')

    def forward(self, x):
        return self.model.forward(x)

    @staticmethod
    def get_weights_filename():
        return f'segmentator_v{config["segmentator_model_version"]}.pt'
