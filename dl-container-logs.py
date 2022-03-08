import argparse
import multiprocessing
import requests
from requests_kerberos import HTTPKerberosAuth
import re
import os

simulation = False

def main(tracking_url):
	tracking_html_path = 'logs/tracking-url.html'
	response = requests.get(tracking_url, stream=True, auth=HTTPKerberosAuth())
	os.makedirs(os.path.dirname(tracking_html_path), exist_ok=True)
	with open(tracking_html_path, 'wb') as log_file:
		for chunk in response.iter_content(chunk_size=int(1E7)):
			if chunk:
				log_file.write(chunk)

	log_url_regex = "<a href='(http:\/\/[a-z0-9\/\.:\-_]*\/kafkaetl)'>Logs</a>"
	log_urls = [f'{url[0]}/GobblinYarnTaskRunner.stdout/?start=0' for line in open(tracking_html_path) if (url:=re.findall(log_url_regex, line))]
	container_regex = "containerlogs/([a-z_0-9]+)/kafkaetl"

	m = multiprocessing.Manager()
	tasksCompleted = m.Value('i', 0)
	lock = m.Lock()
	totalTasks = len(log_urls)
	tasks = [(
		url,
		re.findall(container_regex, url)[0],
		tasksCompleted,
		lock,
		totalTasks
		) for url in log_urls]
	with multiprocessing.Pool(processes=8) as pool:
		pool.starmap(download, tasks)
	print(f'Done downloading logs for {totalTasks}/{totalTasks} containers')

def download(url, container, tasksCompleted, lock, totalTasks):
	global simulation
	log_path=f'logs/container-logs/{container}.out'
	os.makedirs(os.path.dirname(log_path), exist_ok=True)
	with open(log_path, 'wb') as log_file:
		if not simulation:
			with requests.get(url, stream=True, auth=HTTPKerberosAuth()) as response:
				for chunk in response.iter_content(chunk_size=int(1E7)):
					if chunk:
						log_file.write(chunk)

	with lock:
		tasksCompleted.value += 1
		print(f'{tasksCompleted.value}/{totalTasks}', end = "\r")

if __name__ == '__main__':
	parser = argparse.ArgumentParser(description='script argument parser')
	parser.add_argument('app_attempt_url', help="URL of hadoop app attempt tracking. This URL is obtained by clicking on the attempt id of the tracking_url from Azkaban")
	parser.add_argument('-s', '--simulation',
			help='Simulation mode on whether to download all log files from tracking url', action="store_true")
	args = parser.parse_args()
	simulation = args.simulation
	main(args.tracking_url)