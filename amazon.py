import requests
import json
import gc
from scrapy.http import HtmlResponse
from urllib.parse import urlencode, unquote
import pytz
from datetime import datetime, timezone
from cleantext import clean
import urllib3

urllib3.disable_warnings()

def get_page_response_scop(url, postmethod, data, cookies, proxy, maxRetry, zipcode):
    """
    Handles requests using ScrapeOps API for different country-specific requests.
    """
    try:
        retry = 1
        Flag = True

        while Flag and retry <= maxRetry:
            try:
                # If the method is GET, handle requests accordingly based on the zipcode
                if postmethod == "get":
                    try:
                        # Handling ES zip codes
                        if "_es" in zipcode:
                            proxy_params = {
                                'api_key': 'Demo_Key',
                                'url': f'{url}',
                                'country': 'es',
                            }

                            response = requests.get(
                                url='https://proxy.scrapeops.io/v1/',
                                params=urlencode(proxy_params),
                                timeout=60,
                            )

                        # Handling PT zip codes
                        elif "_pt" in zipcode:
                            proxy_params = {
                                'api_key': 'Demo_Key',
                                'url': f'{url}',
                                'country': 'pt',
                            }

                            response = requests.get(
                                url='https://proxy.scrapeops.io/v1/',
                                params=urlencode(proxy_params),
                                timeout=60,
                            )

                    except Exception as e:
                        print(f"Error: {e}")
                        print(response.json())

                    # Terminate loop if valid status codes are returned
                    if response.status_code in [200, 401, 404] or "Sorry, that page doesn't exist." in response.text:
                        Flag = False
                else:
                    print("Rotate to next IP")

            except Exception as ex:
                if 'ProxyError' in str(ex) or 'SSLError' in str(ex):
                    False
                else:
                    print(ex)

            retry += 1

        gc.collect()
        return response.text, response

    except Exception as E:
        print(f"Error in response function: {E}")

def get_page_response_oxylab(url, postmethod, data, cookies, proxy, maxRetry, zipcode):
    """
    Handles requests using Oxylabs API for different country-specific requests.
    """
    try:
        retry = 1
        Flag = True

        while Flag and retry <= maxRetry:
            try:
                if postmethod == "get":
                    try:
                        # Handle ES requests
                        if "_es" in zipcode:
                            payload = {
                                'source': 'amazon',
                                'url': f'{url}',
                                'geo_location': 'ES',
                                'domain': 'es',
                                'locale': 'es_ES',
                                'parse': False
                            }

                            response = requests.request(
                                'POST',
                                'https://realtime.oxylabs.io/v1/queries',
                                auth=('user', 'password'),
                                json=payload,
                            )

                        # Handle PT requests
                        elif "_pt" in zipcode:
                            payload = {
                                'source': 'amazon',
                                'url': f'{url}',
                                'geo_location': 'PT',
                                'domain': 'es',
                                'locale': 'pt_PT',
                                'parse': False
                            }

                            response = requests.request(
                                'POST',
                                'https://realtime.oxylabs.io/v1/queries',
                                auth=('user', 'password'),
                                json=payload,
                            )
                    except Exception as e:
                        print(f"Error: {e}")
                        print(response.json())

                    print(response.text)

                    if response.status_code in [200, 401, 404] or "Sorry, that page doesn't exist." in response.text:
                        Flag = False
                else:
                    print("Rotate to next IP")

            except Exception as ex:
                if 'ProxyError' in str(ex) or 'SSLError' in str(ex):
                    False
                else:
                    print(ex)

            retry += 1

        gc.collect()
        response1 = json.loads(response.text)
        decoded_html = response1['results'][0]['content']
        return decoded_html, response

    except Exception as E:
        print(f"Error in response function: {E}")

def makeNone(item):
    """
    Returns an empty string if the item is None.
    """
    return item if item else ''

def parseData(resp, input, url, rvsp, asin):
    """
    Parses the HTML response from Amazon product pages.
    Extracts product details like price, stock, shipping info, etc.
    """
    import datetime
    dt = datetime.datetime.now(timezone.utc)
    utc_timestamp = dt.replace(tzinfo=timezone.utc).timestamp()

    if not resp or rvsp.status_code == 404:
        input['crawled_time'] = str(utc_timestamp).split('.')[0]
        input["statuscode"] = rvsp.status_code
        return input

    response = HtmlResponse(url="my HTML string", body=resp, encoding='utf-8')

    # Extract breadcrumb path
    breadcrumbs = [unquote(x).replace('\n', '').strip() for x in response.xpath('//div[@id="wayfinding-breadcrumbs_feature_div"]//li/span[@class="a-list-item"]//a//text()').extract() if x.strip()]
    path = ' > '.join(breadcrumbs)

    # Extract product title
    t_ = response.xpath('//h1[@id="title"]//text()').extract()

    try:
        checkCaptcha = t_[0]
    except:
        checkCaptcha = None

    if not checkCaptcha:
        input['crawled_time'] = str(utc_timestamp).split('.')[0]
        input["statuscode"] = 403
        return input

    product_title = ' '.join([x.rstrip().strip() for x in t_ if x.rstrip().strip()])

    # Extract image URL
    img_url = response.xpath('//div[@id="imgTagWrapperId"]//img/@src').extract_first() or ''

    # Handle pricing for amazon_pt
    if input['zipcode'] == 'amazon_pt':
        try:
            promo_price = response.xpath('//div[@id="corePrice_feature_div"]//span[contains(@class,"a-price ")]//span[@class="a-offscreen"]/text()').extract_first().strip().replace(',', '.').replace('€', '').strip().replace('\xa0', '')
        except:
            promo_price = ""

        try:
            regular_price = response.xpath("//span[@class='a-size-small a-color-secondary aok-align-center basisPrice']/span/span[@class='a-offscreen']/text()").extract_first().replace('€', '').strip()
        except:
            regular_price = ""

    # Handle pricing for amazon_es
    elif input['zipcode'] == 'amazon_es':
        try:
            promo_price = response.xpath('//div[@id="corePrice_feature_div"]//span[contains(@class,"a-price ")]//span[@class="a-offscreen"]/text()').extract_first().strip().replace('.', '').replace(',', '.').replace('€', '').strip().replace(' ', '').replace('\xa0', '')
        except:
            promo_price = ""

        try:
            regular_price = response.xpath("//span[@class='a-size-small a-color-secondary aok-align-center basisPrice']/span/span[@class='a-offscreen']/text()").extract_first().replace('€', '').strip()
        except:
            regular_price = ""

    # Check if regular price is the same as promo price
    if regular_price and not promo_price:
        promo_price = regular_price
        regular_price = ""

    if promo_price == regular_price:
        promo_price = regular_price
        regular_price = ""

    # Extract brand name
    try:
        brand_name = response.xpath('//a[@id="bylineInfo"]//text()').extract_first().replace('Visita la tienda de', '').split('(')[0].strip()
    except:
        brand_name = ''

    # Extract seller information
    try:
        seller = response.xpath("//*[@id='sellerProfileTriggerId']//text() | //*[@data-csa-c-content-id='desktop-merchant-info']//span[@class='a-size-small offer-display-feature-text-message']/text()").extract_first()
    except:
        seller = ""

    # Extract shipping details
    try:
        shipping_details = response.xpath('//div[contains(@id,"mir-layout-DELIVERY_BLOCK-slot-PRIMARY_DELIVERY_MESSAGE_LARGE")]//text()').extract()
        shipping_details = ' '.join([x.strip() for x in shipping_details if x.strip()])
    except:
        shipping_details = ""

    # Extract delivery times
    try:
        delivery_time_max = response.xpath('//div[contains(@id,"mir-layout-DELIVERY_BLOCK-slot-PRIMARY_DELIVERY_MESSAGE_LARGE")]//span//@data-csa-c-delivery-time').extract_first()
    except:
        delivery_time_max = ""

    try:
        delivery_time_min = response.xpath('//div[contains(@id,"mir-layout-DELIVERY_BLOCK-slot-SECONDARY_DELIVERY_MESSAGE_LARGE")]//span//@data-csa-c-delivery-time').extract_first()
    except:
        delivery_time_min = ""

    # Determine the final shipping price and stock status
    try:
        spain_timezone = pytz.timezone('Europe/Madrid')
        current_time_in_spain = datetime.now(spain_timezone).strftime("%Y-%m-%d %H:%M:%S.%f%z")
    except:
        current_time_in_spain = ""

    try:
        stock = response.xpath('//div[@id="availability"]//text()').extract()
        if stock:
            stock = "In Stock"
        else:
            stock = "Out of Stock"
    except:
        stock = "Out of Stock"

    # Final product data return
    retData = {
        "EAN": input['EAN'],
        "Product_Name": product_title,
        "Brand": brand_name.strip() if brand_name else '',
        "Path": path,
        "Category": breadcrumbs[0] if breadcrumbs else '',
        "Website": input['zipcode'],
        "Dealer": clean(makeNone(seller), no_emoji=True, lower=False),
        "Url": f"https://www.amazon.es/dp/{asin}",
        "Stock": stock,
        "Price": '%.2f' % float(promo_price) if promo_price else '',
        "Shipping": makeNone(''),
        "Price_Shipping": '%.2f' % float(promo_price) if promo_price else '',
        "Promo": '%.2f' % float(regular_price) if regular_price else '',
        "Delivery_time_Min": makeNone(delivery_time_min),
        "Delivery_time_Max": makeNone(delivery_time_max),
        "Date_Recolha": current_time_in_spain,
        "DeliveryInfo": makeNone(shipping_details).strip(),
        "Marketplace": "Yes" if "amazon" not in seller.lower() else "No",
        "crawled_time": str(utc_timestamp).split('.')[0],
        "statuscode": rvsp.status_code
    }

    return retData

def parseUrl(response, input, rps):
    """
    Parse the Amazon URL to extract the ASIN.
    """
    try:
        applySelector_ = json.loads(response)
        asin = applySelector_['data']['otherProducts']['products'][0]['asin']
        url = f"https://www.amazon.es/dp/{asin}" if asin else None
    except:
        url = None

    return {"EAN": input['Url'], "Url": url, "statuscode": rps.status_code}, asin

def getProductData(input):
    """
    Main function to fetch product data from Amazon using proxy requests.
    """
    url = input['Url']

    if "https:" not in input['Url']:
        urlData, asin = getUrl(input)
        input["EAN"] = url
        url = urlData.get('Url', None)

        if urlData['statuscode'] == 200 and not url:
            dt = datetime.now(timezone.utc)
            utc_time = dt.replace(tzinfo=timezone.utc).timestamp()
            input["Url"] = ""
            input['crawled_time'] = str(utc_time).split('.')[0]
            input["statuscode"] = 404
            return input

    if '/dp/' in str(url):
        asin = str(url).split('/dp/')[1].split('?')[0].strip()

    zipcode = input.get('zipcode', '')

    if '_pt' in zipcode:
        cookies = {
            'session-id': '257-9636201-4216623',
            'i18n-prefs': 'EUR',
            'ubid-acbes': '260-9923205-4133856',
            'session-id-time': '2082787201l',
            'lc-acbes': 'pt_PT',
            'session-token': 'Z/rrzj/aMvg8o3oMEzzHwB+t9UeRbKK7bALOTHINvWlyT2CSTxdz3doVDpr9PLscBcu5c6r/kcoKhsJW1oxm9shVl8ucdzRjvSc40WUgvpyfzw68PWpvnM16L7vbXQcGdrNqLFdT9vn8F7+aC0J4EYrUDpF88776BeorZRjTlaf66KXvkJoLspiG8p5/6EFY3r8F/gaX43yRFJBp1YO1rpEI7HSLSwxY99IhDueSji24ljeVALee5Igv/mA5qFgAc5FioR48GGohQEbgiB8BG6OhJ1AKDjBOY9yghZXg71Kgf+q0SYZ2XGR11QIwFmnXjJYficxwSm9cgVKO/AMhpWpmdkm7uMHV',
        }

    elif '_es' in zipcode:
        cookies = {
            'session-id': '257-9636201-4216623',
            'i18n-prefs': 'EUR',
            'ubid-acbes': '260-9923205-4133856',
            'session-id-time': '2082787201l',
            'session-token': 'jVj5Ibw4VDcmhUmMRvDDNx5Yab5lPwu+Hw0EqU8YNok5krRqIf1QTh019yB4uOhn9aYJerFTO9JvFX1PZdydl7VYShfg0i5HKiCErIHqvQUZr2TBPK//AewqsO/VcDE5FJqTQbelHWUjjy6rKRUCf9GAATQeqg76DZYIwobqeHvYVGBdERtbIOkjuC9gmkuqztzH92iPUQM2mCwIlp4iVHJFMiIn06A/BkGwRiuMfST2HTO4zvjAJTQ8b5t8PA47yhSTTYam+/Bbb1hVEMjPQAgGQDr1gurxN7Vmx7Y4HCnDnGNPIxrcOeazDp6Ojan8uv6voCRKcap3piwN2DTJUlQnGgUxHqPC',
        }

    response, rvsp = get_page_response_oxylab(url, 'get', '', cookies, input['proxies'], 5, zipcode)
    
    return parseData(response, input, url, rvsp, asin)

def getUrl(input):
    """
    Fetch URL using Oxylabs proxy and get the ASIN for Amazon products.
    """
    cks = {
        'session-id': '257-9636201-4216623',
        'i18n-prefs': 'EUR',
        'ubid-acbes': '260-9923205-4133856',
        'session-id-time': '2082787201l',
        'lc-acbes': 'pt_PT',
        'session-token': 'jVj5Ibw4VDcmhUmMRvDDNx5Yab5lPwu+Hw0EqU8YNok5krRqIf1QTh019yB4uOhn9aYJerFTO9JvFX1PZdydl7VYShfg0i5HKiCErIHqvQUZr2TBPK//AewqsO/VcDE5FJqTQbelHWUjjy6rKRUCf9GAATQeqg76DZYIwobqeHvYVGBdERtbIOkjuC9gmkuqztzH92iPUQM2mCwIlp4iVHJFMiIn06A/BkGwRiuMfST2HTO4zvjAJTQ8b5t8PA47yhSTTYam+/Bbb1hVEMjPQAgGQDr1gurxN7Vmx7Y4HCnDnGNPIxrcOeazDp6Ojan8uv6voCRKcap3piwN2DTJUlQnGgUxHqPC',
    }

    Cookie_Templete = {
        "domain": "www.amazon.es",
        "httpOnly": False,
        "path": "/",
        "sameSite": "Lax",
        "secure": False,
    }

    cookies_N = [
        {**Cookie_Templete, "name": name, "value": value}
        for name, value in cks.items()
    ]

    response_html, response = get_page_response_oxylab(f"https://sellercentral.amazon.es/rcpublic/productmatch?searchKey={input['Url']}&countryCode=ES&locale=en-ES", 'get', {}, cookies_N, input['proxies'], 5, "es")
    gc.collect()

    return parseUrl(response_html, input, response)
