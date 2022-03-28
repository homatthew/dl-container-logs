import argparse
import multiprocessing
import requests
from requests_kerberos import HTTPKerberosAuth
import re
import os

simulation = None

def main(tracking_url, num_processes, proxy_user):
  global simulation
  app_attempt_html_path = 'logs/app-attempt.html'
  response = requests.get(tracking_url, stream=True, auth=HTTPKerberosAuth())
  os.makedirs(os.path.dirname(app_attempt_html_path), exist_ok=True)
  with open(app_attempt_html_path, 'wb') as log_file:
    for chunk in response.iter_content(chunk_size=int(1E7)):
      if chunk:
        log_file.write(chunk)

  log_url_regex = f"<a href='(http:\/\/[a-z0-9\/\.:\-_]*\/{proxy_user})'>Logs</a>"
  log_urls = [f'{url[0]}/GobblinYarnTaskRunner.stdout/?start=0' for line in open(app_attempt_html_path) if (url:=re.findall(log_url_regex, line))]

  m = multiprocessing.Manager()
  tasksCompleted = m.Value('i', 0)
  lock = m.Lock()
  totalTasks = len(log_urls)
  container_regex = f"containerlogs/([a-z_0-9]+)/{proxy_user}"
  tasks = [(
    url,
    re.findall(container_regex, url)[0],
    tasksCompleted,
    lock,
    totalTasks,
    simulation
    ) for url in log_urls]
  with multiprocessing.Pool(processes=num_processes) as pool:
    pool.starmap(download, tasks)
  print(f'Done downloading logs for {totalTasks}/{totalTasks} containers')

def download(url, container, tasksCompleted, lock, totalTasks, simulation):
  log_path=f'logs/container-logs/{container}.html'
  os.makedirs(os.path.dirname(log_path), exist_ok=True)
  with open(log_path, 'wb') as log_file:
    if not simulation:
      with requests.get(url, stream=True, auth=HTTPKerberosAuth()) as response:
        for chunk in response.iter_content(chunk_size=int(1E7)):
          if chunk:
            log_file.write(chunk)
    else:
      print(f'getting {url} and writing to {log_path}')

  with lock:
    tasksCompleted.value += 1
    print(f'{tasksCompleted.value}/{totalTasks}', end = "\r")

if __name__ == '__main__':
  parser = argparse.ArgumentParser(description='script argument parser')
  parser.add_argument('app_attempt_url', help="URL of hadoop app attempt tracking. This URL is obtained by clicking on the attempt id of the tracking_url from Azkaban")
  parser.add_argument('-s', '--simulation', default=False,
      help='Simulation mode on whether to download all log files from tracking url', action="store_true")
  parser.add_argument('-p', '--processes', default=16,
      type=int, help="number of parallel processes used for downloading")
  parser.add_argument('-u', '--user', default="kafkaetl", help="proxy user for the job")

  args = parser.parse_args()
  simulation = args.simulation
  main(args.app_attempt_url, args.processes, args.user)