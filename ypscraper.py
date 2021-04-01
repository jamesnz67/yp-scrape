import csv
import concurrent.futures
import os
import atexit
import collections
import time
import datetime
import threading
import sys
import logging
import tempfile

import fire
import requests
from requests.exceptions import *
from bs4 import BeautifulSoup

class CsvWriter_:
	def __init__(self, outfile_name):
		if os.path.isfile(outfile_name):
			os.remove(outfile_name)
		self.outfile = open(outfile_name, 'w')
		self.csv_writer = csv.DictWriter(self.outfile, fieldnames=['business_name', 'description', 'post_code', 'address', 'ph', 'email', 'website', 'listing_url'])
		self.csv_writer.writeheader()
		self.writer_lock = threading.Lock()
		atexit.register(self.cleanup)

	def write_dict(self, data):
		with self.writer_lock:
			self.csv_writer.writerow(data)
		
	def cleanup(self):
		self.outfile.close()


class YpScraper:
	def __init__(self, max_conn=100, search_query='Handyman', location='All States'):
		self.csv_writer = CsvWriter_(''.join(x if x.isalnum() else '_' for x in search_query) + f'_{datetime.datetime.now():%Y-%m-%d}' + '.csv') #pass in a file-friendly name
		#self.search_query = search_query.title().replace(' ', '+') ##
		#self.location = location.title().replace(' ', '+') ##
		#self.location = location #can be postcode too passed in as string
		self.load_proxies()
		self.num_generator = (x for x in range(1000000000000000)) #enumerate possible page numbers, stops being called on the first 'no results found'
		self.num_gen_lock = threading.Lock()
		self.stdout_lock = threading.Lock()

		#https://www.yellowpages.com.au/search/listings?clue=Handyman&locationClue=All+States&pageNumber=5
		logging.basicConfig(level=logging.DEBUG)
		start = time.time()
		with concurrent.futures.ThreadPoolExecutor(max_workers=max_conn) as executor:
				futures = [executor.submit(self.start_scraper, search_query, location) for _ in range(max_conn)]
				#results = {result for result in concurrent.futures.as_completed(futures)} 
				for future in concurrent.futures.as_completed(futures):
					if e := future.exception():
						logging.error(f'future exception - {e}')
		hours, rest = divmod(time.time() - start, 3600)
		minutes, seconds = divmod(rest, 60)
		sys.exit(f'Process finished in: {int(minutes)} mins and {int(seconds)} sec')


	def get_pagenum(self):
		with self.num_gen_lock:
			return next(self.num_generator)

#search_query, location
	def start_scraper(self, search_query, location):
		page_num = self.get_pagenum()
		proxy = self.proxies.popleft()
		while isinstance(response := self.get_response(proxy, search_query, location, page_num), (str, int)):
			if isinstance(response, str):
				data = self.process_response(response)
				if isinstance(data, list):
					logging.debug(f'listings are {data} page num is {page_num}')
					for row in data:
						self.csv_writer.write_dict(row)
					page_num = self.get_pagenum()
					continue
				elif data == 'captcha':
					logging.debug(f'got captcha for {proxy}, trying new proxy')
					self.proxies.append(proxy)				
				elif data == 'end':
					logging.debug(f'reached end on page_num {page_num} for thread {threading.current_thread().getName()}')
					break		
			proxy = self.proxies.popleft()	
			#default connectionerror = retry with same pagenum
		else:
			logging.debug(f'resp is {response}')	
		with self.stdout_lock:
			logging.debug(f'shutting down thread {threading.current_thread().getName()}')

	def load_proxies(self):
		with open('proxies.txt', 'r', encoding='utf-8') as proxylist_file:
			self.proxies = collections.deque({f'http://{proxy.strip()}' for proxy in proxylist_file})
	

	def get_response(self, proxy, search_query, location, page_num):
		url = 'https://www.yellowpages.com.au/search/listings'
		params = {'referredBy': 'www.yellowpages.com.au', 'clue': search_query, 'locationClue': location, 'pageNumber': page_num}

		headers = {
		'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; rv:68.0) Gecko/20100101 Firefox/68.0',
		'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8',
		'Accept-Language': 'en-US,en;q=0.5',
		#'Referer': 'https://www.yellowpages.com.au/search/listings?clue=handyman&eventType=pagination&pageNumber=6&referredBy=www.yellowpages.com.au',
		'DNT': '1',
		'Connection': 'keep-alive',
		'Upgrade-Insecure-Requests': '1',
		'TE': 'Trailers',
		}

		try:
			r = requests.get(url, params=params, headers=headers, proxies={'https': proxy}, timeout=15)
			if r.status_code == 200:
				return r.text
			else:
				logging.debug(f'got non-200 status code {r.status_code}, retrying page_num {page_num}')
		except (ReadTimeout, ConnectTimeout, SSLError, ConnectionError) as e:
			logging.debug(e)
		except Exception as e:
			logging.warning(f'unexpected exception {e}')	
		return page_num


	def process_response(self, response):
		soup = BeautifulSoup(response, features="html.parser")
		#if soup.find('div', class_='g-recaptcha'):
		#	return 'captcha'
		if (title := soup.find('title')) and title.text == "Yellow Pages® | Data Protection":
			return 'captcha'
			
		listings = []
		for listing in soup.find_all('div', class_="listing listing-search listing-data"):
			description = listing.find('p', class_='listing-heading')
			address = listing.find('p', class_='listing-address')
			ph = listing.find('span', class_='contact-text')
			email = listing.find('a', class_='contact-email')
			website = listing.find('a', class_='contact-url')

			listings.append({
				'post_code': listing.attrs.get('data-postcode'),
				'business_name': listing.attrs.get('data-full-name'),
				'description': description.a.text if (description and description.a) else None,
				'address': address.text if address else None,
				'ph': ph.text if ph else None,
				'email': email.attrs.get('data-email') if email else None,
				'website': website.attrs.get('href') if website else None
			})
		if not listings:
			if div := soup.find('div', class_='search-result-message'):
				if 'No results found for' in div.text:
					#no more pages past this page number
					return 'end'		
			with tempfile.NamedTemporaryFile(mode='w', encoding='utf-8', delete=False) as unexpected_result:
				unexpected_result.write(response)
				logging.warning(f'got 0 listings for page saved to {unexpected_result.name}')
		
		return listings
		

if __name__ == '__main__':
	fire.Fire(YpScraper)

