import logging
def create_log_object(county_name, start_index, end_index, log_file_name_refex=''):
    log_file_name = f"logs/debug_{county_name}_{start_index}_{end_index}.log"
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