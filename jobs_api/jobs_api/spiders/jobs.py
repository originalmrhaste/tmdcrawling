import math
import pickle
import json
import scrapy
import itertools

with open('industry_ids.pickle', 'rb') as f:
    INDUSTRY_IDS = pickle.load(f)
with open('category_ids.pickle','rb') as f:
    CATEGORY_IDS = pickle.load(f)
#INDUSTRY_IDS = [10,2,13,6,12,16,22,1,5,19,23,7,18,4,20,11,9,21,17,8,15,14,3,24]
#REGION_IDS = [19,20,22,25,17,18,21,23,24,3,4,5,16,12,14,13,15]

class JobsSpider(scrapy.Spider):
    """Spider for scraping api of jobs.ch"""
    name = 'jobs'

    def start_requests(self):
        """Populate starting urls with api calls
        containg combination of industry and category ids"""

        industry_category = itertools.product(INDUSTRY_IDS, CATEGORY_IDS)
        urls = {
                    (industry_id,category_id):
                    f'https://www.jobs.ch/api/v1/public/search?industry-ids[]={industry_id}'
                    f'&category-ids[]={category_id}&page=1&rows=100'
                    for industry_id,category_id in industry_category
                }

        for ids,url in urls.items():
            yield scrapy.Request(
                        url=url,
                        callback=self.parse_first,
                        cb_kwargs={'industry_id':ids[0],'category_id':ids[1]}
                        )

    def parse_first(self, response, industry_id, category_id):
        """Parse the first page and yield documents,
        Generate additional requests based on how many hits we got"""

        json_response = json.loads(response.body)

        # yield items from this page
        for document in json_response['documents']:
            yield document

        # calculate number of pages to crawl
        total_hits = json_response['total_hits']
        pages = math.ceil(total_hits/100)
        base_url = (
                f'https://www.jobs.ch/api/v1/public/search?industry-ids[]={industry_id}'
                f'&category-ids[]={category_id}&rows=100'
                )
        rest_urls = [base_url+f'&page={i}' for i in range(2,min(pages+1,21))]

        # generate requests
        for url in rest_urls:
            yield scrapy.Request(url=url, callback=self.parse_rest)

    def parse_rest(self, response):
        """Yield every document in additional requests"""

        json_response = json.loads(response.body)
        for document in json_response['documents']:
            yield document
