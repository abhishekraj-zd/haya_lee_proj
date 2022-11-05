import logging
def create_log_object(county_name, log_file_name_refex=''):
    log_file_name = f"logs/debug_{county_name}.log"
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [%(levelname)s] %(message)s",
        handlers=[
            logging.FileHandler(log_file_name),
            # logging.StreamHandler()
        ]
    )
    rootLogger = logging.getLogger()
    return rootLogger