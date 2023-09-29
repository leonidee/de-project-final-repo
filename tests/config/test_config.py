import sys
import pprint
import yaml

with open('/home/leonide/code/de-project-final-repo/src/transaction_service_stream_collector/config.yaml')as f:
    config = yaml.safe_load(f)


def main():


    pprint.pprint(config['app']['prod']['output']['path'])



if __name__ == "__main__":
    main()
