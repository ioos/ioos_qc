#!python
from ioos_qc.config_creator import (
    CreatorConfig,
    QcConfigCreator,
    QcVariableConfig,
    QC_CONFIG_CREATOR_SCHEMA,
    VARIABLE_CONFIG_SCHEMA
)


if __name__ == '__main__':
    import argparse

    parser = argparse.ArgumentParser()
    parser.add_argument(
        'creator_config_file',
        type=str,
        help='path to creator config file (e.g. creator_config.json)'
    )
    parser.add_argument(
        'variable_config_file',
        type=str,
        help='path to variable config file (e.g. air_config.json)'
    )
    args = parser.parse_args()

    creator_config = CreatorConfig(args.creator_config_file, QC_CONFIG_CREATOR_SCHEMA)
    variable_config = QcVariableConfig(args.variable_config_file, VARIABLE_CONFIG_SCHEMA)
    qc = QcConfigCreator(creator_config)
    qc.create_config(variable_config)
