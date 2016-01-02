import logging
import os
from Queue import Queue
import threading

import argparse

LOG = logging.getLogger(__name__)
logging.basicConfig(format='%(asctime)s - %(levelname)s - %(message)s', level=logging.INFO)

class LineCounterWorker(threading.Thread):
    def __init__(self, worker_id, processing_queue, result_queue):
        self.worker_id = worker_id
        self.processing_queue = processing_queue
        self.results = result_queue
        threading.Thread.__init__(self)

    def run(self):
        while True:
            if processing_queue.empty():
                LOG.debug("Worker: %s. Found out queue is empty. Exiting", self.worker_id)
                return
            file_name = self.processing_queue.get()
            with open(file_name) as f:
                lines_count = len(f.readlines())
                self.results.put(lines_count)
                LOG.info("Worker: %s, File: %s, lines: %s", self.worker_id, file_name, lines_count)
                self.processing_queue.task_done()

def config_arg_parser(args=None):
    parser = argparse.ArgumentParser()
    parser.add_argument('-d', dest="directory")
    parser.add_argument('-f', dest="filter")
    parser.add_argument('-n', dest="num_processors", type=int, required=False)

    if args:
        res = parser.parse_args()
    else:
        res = parser.parse_args(args)
    return res

def accumulate(queue_of_ints):
    total = 0
    while not queue_of_ints.empty():
        total += queue_of_ints.get()
    return total


def list_files(root_dir, extension):
    found_files = []
    for file_name in os.listdir(root_dir):
        if file_name.endswith(extension):
            found_files.append(os.path.join(root_dir, file_name))
    LOG.debug("Found files %s in '%s' directory", str(found_files), root_dir)
    return found_files


if __name__ == '__main__':
    cli_args = config_arg_parser()
    LOG.debug(cli_args)

    LOG.info("Dist counter started.Looking at '%s' directory for '%s' files", cli_args.directory, cli_args.filter)
    files = list_files(cli_args.directory, cli_args.filter)

    processing_queue = Queue()
    [processing_queue.put(f) for f in files]

    num_of_workers = cli_args.num_processors if cli_args.num_processors else processing_queue.qsize()/2
    LOG.debug("Starting %s workers based on queue size %s", num_of_workers, processing_queue.qsize())

    results_collector = Queue()
    for i in range(1, num_of_workers+1):
        worker = LineCounterWorker(i, processing_queue, results_collector)
        worker.daemon = True
        worker.start()
        LOG.debug("Fired up worker %s/%s", i, num_of_workers)

    processing_queue.join()
    LOG.info("Done workload processing")

    total = accumulate(results_collector)
    LOG.info("Total lines: %s", total)
