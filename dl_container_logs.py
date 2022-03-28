import argparse
import multiprocessing
import requests
from requests_kerberos import HTTPKerberosAuth
import re
import os
import shutil
import datetime
import zoneinfo

simulation = None

def main(tracking_url, num_processes, proxy_user, date):
  global simulation
  response = requests.get(tracking_url, auth=HTTPKerberosAuth())
  response.raise_for_status()
  log_urls = get_log_urls(tracking_html=response.text.split('\n'), proxy_user=proxy_user, date=date, num_processes=num_processes)
  download_all(log_urls, proxy_user, num_processes)

def get_log_urls(tracking_html, proxy_user, date, num_processes):
  log_url_regex = f"<a href='(http:\/\/[a-z0-9\/\.:\-_]*\/{proxy_user})'>Logs</a>"
  if date is None:
    log_urls = [f'{url[0]}/GobblinYarnTaskRunner.stdout/?start=0' for line in tracking_html if (url:=re.findall(log_url_regex, line))]
  else:
    container_urls = [url[0] for line in tracking_html if (url:=re.findall(log_url_regex, line))]
    log_urls = get_logs_by_date(container_urls, date, num_processes)
  return log_urls

def get_logs_by_date(container_urls, date, num_processes):
  m = multiprocessing.Manager()
  q = m.Queue()
  with multiprocessing.Pool(processes=num_processes) as pool:
    tasks = [(url, date, q) for url in container_urls]
    pool.starmap(get_container_log_urls, tasks)
    q.put(None) # termination signal

  return [url for url in iter(q.get, None)]

def get_container_log_urls(url, date, return_log_urls):
  response = requests.get(url, auth=HTTPKerberosAuth())
  response.raise_for_status()

  date_suffix_regex = f'<a href="\/node\/containerlogs\/.*\/GobblinYarnTaskRunner\.stdout(\.{date}[0-9\-\.]+)\/\?start='
  container_log_urls = [f'{url}/GobblinYarnTaskRunner.stdout{date_suffix[0]}/?start=0' for line in response.text.split('\n') if (date_suffix:=re.findall(date_suffix_regex, line))]

  if len(container_log_urls) < 24 and itIsToday(date):
    # download the most recent hour's data too
    container_log_urls.append(f'{url}/GobblinYarnTaskRunner.stdout/?start=0')
  for url in container_log_urls:
    return_log_urls.put(url)


def download_all(log_urls, proxy_user, num_processes):
  global simulation
  m = multiprocessing.Manager()
  tasksCompleted = m.Value('i', 0)
  lock = m.Lock()
  totalTasks = len(log_urls)
  container_regex = f"containerlogs/([a-z_0-9]+)/{proxy_user}/GobblinYarnTaskRunner\.stdout(.*)/\?start"
  tasks = [(
    url,
    (s:=re.search(container_regex, url)).group(1) + "/" + (s.group(2)[1:] if s.group(2) else "latest"),
    tasksCompleted,
    lock,
    totalTasks,
    simulation
    ) for url in log_urls]
  with multiprocessing.Pool(processes=num_processes) as pool:
    pool.starmap(download, tasks)
  print(f'Done downloading logs for {totalTasks}/{totalTasks} containers')

def download(url, file_name, tasksCompleted, lock, totalTasks, simulation):
  log_path=f'logs/container-logs/{file_name}.html'
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

def itIsToday(isoDate):
  gridtz = zoneinfo.ZoneInfo('America/Los_Angeles')

  today = datetime.datetime.now(tz=datetime.timezone.utc).astimezone(gridtz)
  given_date = datetime.datetime.fromisoformat(isoDate).date()
  return today.date() == given_date

if __name__ == '__main__':
  parser = argparse.ArgumentParser(description='script argument parser')
  parser.add_argument('app_attempt_url', help="URL of hadoop app attempt tracking. This URL is obtained by clicking on the attempt id of the tracking_url from Azkaban")
  parser.add_argument('-s', '--simulation', default=False,
      help='Simulation mode on whether to download all log files from tracking url', action="store_true")
  parser.add_argument('-p', '--processes', default=16,
      type=int, help="number of parallel processes used for downloading")
  parser.add_argument('-u', '--user', default="kafkaetl", help="proxy user for the job")
  parser.add_argument('-d', '--date', default=None, help="The date you want to download the logs from")
  parser.add_argument('-c', '--clean', default=False, help="Delete all previous log files", action="store_true")

  args = parser.parse_args()
  if re.match("\d{4}-\d{2}-\d{2}", args.date) is None:
    raise argparse.ArgumentTypeError("Date must be in format YYYY-MM-DD")

  if args.clean:
    if os.path.exists("logs"):
      shutil.rmtree("logs")
  simulation = args.simulation
  main(args.app_attempt_url, args.processes, args.user, args.date)