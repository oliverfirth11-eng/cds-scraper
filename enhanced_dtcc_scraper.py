import requests
import time
import logging
from datetime import datetime, timedelta
import os
from supabase import create_client, Client
import re

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class DTCCCDSScraper:
    def __init__(self):
        # Supabase connection
        self.supabase_url = os.getenv('SUPABASE_URL')
        self.supabase_key = os.getenv('SUPABASE_ANON_KEY')
        self.supabase: Client = create_client(self.supabase_url, self.supabase_key)

        # DTCC endpoints
        self.dtcc_base = "https://pddata.dtcc.com"
        self.ppd_dashboard = f"{self.dtcc_base}/ppd/cftcdashboard"

        # HTTP headers to mimic a browser
        self.headers = {
            'Accept': 'application/json, text/plain, */*',
            'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7)'
        }
        self.session = requests.Session()
        self.session.headers.update(self.headers)

    def discover_api_endpoints(self):
        try:
            r = self.session.get(self.ppd_dashboard, timeout=30)
            content = r.text
            patterns = [
                r'fetch\("([^"]*/api/[^"]*)"',
                r'"(/ppd/api/[^"]*)"'
            ]
            endpoints = set()
            for p in patterns:
                for match in re.findall(p, content):
                    if match.startswith("/"):
                        endpoints.add(self.dtcc_base + match)
                    else:
                        endpoints.add(match)
            logger.info(f"Discovered endpoints: {endpoints}")
            return list(endpoints)
        except Exception as e:
            logger.error(f"Endpoint discovery failed: {e}")
            return [f"{self.dtcc_base}/ppd/api/cds/trades"]

    def fetch_cds_trades(self, endpoint):
        params = {
            'product': 'CDS',
            'region': 'EU',
            'limit': 1000,
            'sortBy': 'executionTimestamp',
            'sortOrder': 'desc'
        }
        try:
            r = self.session.get(endpoint, params=params, timeout=30)
            r.raise_for_status()
            data = r.json()
            # Attempts to extract list from known keys
            for key in ['data','trades','records','results']:
                if key in data and isinstance(data[key], list):
                    return data[key]
            if isinstance(data, list):
                return data
        except Exception as e:
            logger.error(f"Fetch failed: {e}")
        return []

    def parse_timestamp(self, s):
        for fmt in ('%Y-%m-%dT%H:%M:%S.%fZ','%Y-%m-%dT%H:%M:%SZ','%Y-%m-%d %H:%M:%S'):
            try:
                dt = datetime.strptime(s, fmt)
                return dt.strftime('%H:%M:%S')
            except: pass
        return datetime.now().strftime('%H:%M:%S')

    def parse_date(self, s):
        for fmt in ('%Y-%m-%d','%d/%m/%Y','%m/%d/%Y'):
            try:
                dt = datetime.strptime(s, fmt)
                return dt.strftime('%Y-%m-%d')
            except: pass
        return datetime.now().strftime('%Y-%m-%d')

    def parse_trade(self, t):
        try:
            return {
                'trade_time': self.parse_timestamp(t.get('executionTimestamp') or ''),
                'ticker': t.get('referenceEntity','UNKNOWN'),
                'name': t.get('issuerName',''),
                'price': float(t.get('price',0) or 0),
                'notional': float(t.get('notionalAmount',0) or 0),
                'code': t.get('tradeType',''),
                'maturity': self.parse_date(t.get('maturityDate') or ''),
                'rate_spr': int((t.get('spread',0) or 0)*100),
                'currency': t.get('currency','EUR'),
                'platform_id': t.get('executionVenue',''),
                'other_payment': float(t.get('upfrontPayment',0) or 0),
                'is_hy': 'HY' in (t.get('ratingClass','') or '').upper(),
                'is_ig': 'IG' in (t.get('ratingClass','') or '').upper()
            }
        except:
            return None

    def scrape_cycle(self):
        logger.info(f"[{datetime.now()}] Starting scrape")
        endpoints = self.discover_api_endpoints()
        all_trades = []
        for ep in endpoints[:2]:
            data = self.fetch_cds_trades(ep)
            if data:
                for raw in data:
                    parsed = self.parse_trade(raw)
                    if parsed: all_trades.append(parsed)
                break
        if all_trades:
            result = self.supabase.table('cds_prices').upsert(all_trades).execute()
            logger.info(f"Stored {len(all_trades)} trades")
        else:
            logger.info("No new trades")
        logger.info("Cycle complete\n")

    def run_forever(self):
        while True:
            try:
                self.scrape_cycle()
                time.sleep(60)
            except KeyboardInterrupt:
                break
            except Exception as e:
                logger.error(f"Unexpected error: {e}")
                time.sleep(30)

if __name__ == "__main__":
    DTCCCDSScraper().run_forever() 
