import pandas as pd
import requests
from bs4 import BeautifulSoup
from concurrent.futures import ThreadPoolExecutor, as_completed
from requests.adapters import HTTPAdapter
from requests.packages.urllib3.util.retry import Retry

# URL of the website
base_url = 'https://www.hcpcsdata.com/Codes'
url = 'https://www.hcpcsdata.com'
headers = {
    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36'
}

# Create a requests session with retry logic
session = requests.Session()
retry = Retry(connect=5, backoff_factor=0.3)
adapter = HTTPAdapter(max_retries=retry)
session.mount('https://', adapter)

def fetch(url, timeout=10):
    """Fetch the content from the URL with a timeout."""
    try:
        response = session.get(url, headers=headers, timeout=timeout)
        response.raise_for_status()
        return response.text
    except requests.exceptions.RequestException as e:
        print(f"Error fetching {url}: {e}")
        return None

def parse_category_page(html):
    """Parse category page to extract links."""
    soup = BeautifulSoup(html, 'html.parser')
    category_rows = soup.find_all('tr', class_='clickable-row')
    links = []
    for row in category_rows:
        link = row.find('a')
        if link:
            href = link.get('href')
            full_url = url + href
            links.append(full_url)
    return links

def parse_details_page(html, group, category):
    """Parse details page to extract code and long description."""
    soup = BeautifulSoup(html, 'html.parser')
    lng_rows = soup.find_all('tr', class_='clickable-row')
    results = []
    for row in lng_rows:
        lng_desc_link = row.find('a')
        lng_td = row.find_all('td')
        if len(lng_td) >= 2 and lng_desc_link:
            code = lng_td[0].text.strip()
            lng_descr = lng_td[1].text.strip()
            short_desc_link = url + lng_desc_link.get('href')
            results.append((group, category, code, lng_descr, short_desc_link))
    return results

def parse_short_desc_page(html):
    """Parse short description page."""
    soup = BeautifulSoup(html, 'html.parser')
    td_elements = soup.find_all('td')
    if len(td_elements) >= 2:
        return td_elements[1].text.strip()
    return ''

def main():
    # Fetch the initial page to get category links
    html = fetch(base_url)
    if not html:
        print("Failed to fetch the base URL.")
        return
    category_links = parse_category_page(html)
    
    all_data = []
    
    with ThreadPoolExecutor(max_workers=20) as executor:  # Reduce the number of workers if needed
        # Fetch all details pages concurrently
        future_to_url = {executor.submit(fetch, link): link for link in category_links}
        details_html = {}
        
        for future in as_completed(future_to_url):
            url = future_to_url[future]
            html = future.result()
            if html:
                details_html[url] = html
        
        # Process details pages
        detail_futures = []
        for link, html in details_html.items():
            group = "HCPCS " + link.split('/')[-1] # Simplified example
            category = "Category from HTML" # You should extract category from detail_html
            details = parse_details_page(html, group, category)
            
            # Fetch short descriptions concurrently
            short_desc_links = [detail[4] for detail in details]
            short_desc_futures = {executor.submit(fetch, link): link for link in short_desc_links}
            short_desc_results = {}
            
            for future in as_completed(short_desc_futures):
                short_url = short_desc_futures[future]
                short_html = future.result()
                if short_html:
                    short_desc = parse_short_desc_page(short_html)
                    short_desc_results[short_url] = short_desc
            
            for detail in details:
                short_desc = short_desc_results.get(detail[4], '')
                all_data.append(detail[:4] + (short_desc,))

                print(all_data)
    
    # Convert results to DataFrame and save to CSV
    df = pd.DataFrame(all_data, columns=['Group', 'Category', 'Code', 'Long Description', 'Short Description'])
    df.to_csv('hcpcs_codes.csv', index=False, encoding='utf-8')

if __name__ == '__main__':
    main()
