import os
import pandas as pd
import numpy as np


class DataloaderFactory:
    def __init__(self, competition_name: str) -> None:
        self.competition_name = competition_name

        def jigsaw_multilingual_toxic_comment_classification(
                cache_path: str) -> dict:

            assert os.path.isdir(
                cache_path), f'directory at {cache_path} does not exists.'
            train = pd.read_csv(
                os.path.join(cache_path, 'jigsaw-toxic-comment-train.csv'))
            unintented_bias_train = pd.read_csv(
                os.path.join(cache_path, 'jigsaw-unintended-bias-train.csv'))
            unintented_bias_train.toxic = unintented_bias_train.toxic.round(
            ).astype(int)

            train_df = pd.concat([
                train[['comment_text', 'toxic']],
                unintented_bias_train[['comment_text',
                                       'toxic']].query('toxic==1'),
                unintented_bias_train[['comment_text',
                                       'toxic']].query('toxic==0').sample(
                                           n=100000, random_state=0)
            ])

            validation_df = pd.read_csv(
                os.path.join(cache_path, 'validation.csv'))

            test_df = pd.read_csv(os.path.join(cache_path, 'test.csv'))

            return {
                'train': train_df,
                'validation': validation_df,
                'test': test_df
            }

        self.loaders = {
            'jigsaw-multilingual-toxic-comment-classification':
            jigsaw_multilingual_toxic_comment_classification
        }

    def get_loader(self):
        return self.loaders[self.competition_name]
