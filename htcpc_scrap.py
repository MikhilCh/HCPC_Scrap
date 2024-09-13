import pandas as pd
import requests
from bs4 import BeautifulSoup
import asyncio
import aiohttp
import csv

base_url = 'https://www.hcpcsdata.com/Codes'
url = 'https://www.hcpcsdata.com'
headers = {
    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36'
}

async def fetch(session, link):
    async with session.get(link) as response:
        return await response.text()

async def fetch_details(session, full_url_lng_desc):
    response_text = await fetch(session, full_url_lng_desc)
    soup_lng_dst = BeautifulSoup(response_text, 'html.parser')
    lng_rows = soup_lng_dst.find_all('tr', class_='clickable-row')

    all_data = []
    for soup_lng_ in lng_rows:
        lng_desc_link = soup_lng_.find('a')  # Link
        lng_td = soup_lng_.find_all('td')
        code = lng_td[0].text.strip()  # Code
        lng_descr = lng_td[1].text.strip()  # Long description

        short_desc_link = url + lng_desc_link.get('href')
        short_desc_response_text = await fetch(session, short_desc_link)
        soup_desc = BeautifulSoup(short_desc_response_text, 'html.parser')
        soup_short = soup_desc.find_all('td')
        if soup_short:
            shortest_desc = soup_short[1].text.strip()

        all_data.append([code, lng_descr, shortest_desc])

    return all_data

async def main():
    async with aiohttp.ClientSession(headers=headers) as session:
        response = await fetch(session, base_url)
        soup = BeautifulSoup(response, 'html.parser')

        category_rows = soup.find_all('tr', class_='clickable-row')
        tasks = []
        for row in category_rows:
            link = row.find('a')
            if link:
                group = f"'HCPCS' {link.text.strip()}"  # Group
                td_elements = row.find_all('td')
                category = td_elements[2].text.strip()  # Category
                full_url_lng_desc = url + link.get('href')

                tasks.append(fetch_details(session, full_url_lng_desc))

        results = await asyncio.gather(*tasks)

        all_data = []
        for i, data in enumerate(results):
            group = f"'HCPCS' {category_rows[i].find('a').text.strip()}"
            category = category_rows[i].find_all('td')[2].text.strip()

            for code, lng_descr, shortest_desc in data:
                all_data.append([group, category, code, lng_descr, shortest_desc])

        df = pd.DataFrame(all_data, columns=['Group', 'Category', 'Code', 'Long Description', 'Short Description'])
        df.to_csv('hcpcs_codes.csv', index=False, encoding='utf-8')

asyncio.run(main())
