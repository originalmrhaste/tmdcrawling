import math
import pickle
import json
import scrapy
import itertools
import requests


class JobsSpider(scrapy.Spider):
    """Spider for scraping api of jobs.ch"""

    name = 'jobs'

    @staticmethod
    def get_ids(facet_key):
        response = requests.get('https://jobs.ch/api/v1/public/search?')
        json_response = json.loads(response.text)
        return list(json_response['facets'][facet_key].keys())

    def __init__(self, *args, **kwargs):
        super(JobsSpider, self).__init__(*args, **kwargs)
        self.num_rows = 100
        self.base_url = f'https://www.jobs.ch/api/v1/public/search?rows={self.num_rows}'
        self.visited = set()
        self.industry_ids = self.get_ids('industry_ids')
        self.category_ids = self.get_ids('category_ids')


    def start_requests(self):
        """Populate starting urls with api calls
        containg combination of industry and category ids"""

        industry_category = itertools.product(self.industry_ids, self.category_ids)

        urls = [
                self.base_url + f'&industry-ids[]={industry_id}&category-ids[]={category_id}'
                for industry_id,category_id in industry_category
                ]

        for url in urls:
            yield scrapy.Request(
                        url=url,
                        callback=self.parse_first
                        )

    def parse_first(self, response):
        """Parse the first page and yield documents,
        Generate additional requests based on how many hits we got"""

        yield from self.parse_rest(response)

        total_hits = json.loads(response.body)['total_hits']

        # calculate number of pages to crawl
        pages = math.ceil(total_hits/self.num_rows)
        rest_urls = [response.url+f'&page={i}' for i in range(2, min(pages+1,21))]

        # generate requests
        for url in rest_urls:
            yield scrapy.Request(url=url, callback=self.parse_rest)


    def parse_rest(self, response):
        """Yield every document in additional requests"""

        json_response = json.loads(response.body)
        for document in json_response['documents']:
            job_id = document['job_id']
            if job_id not in self.visited:
                self.visited.add(job_id)
                yield document
