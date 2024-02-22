import ujson as json
from bs4 import BeautifulSoup
from sparkcc import CCSparkJob
import chardet


class HtmlMetatagCount(CCSparkJob):
    """ Count HTML meta tags. WARC is allowed."""

    name = "CountHtmlMetatag"
    def detect_encoding(self, html_content):
        result = chardet.detect(html_content)
        if result['encoding']:
            return result['encoding']
        else:
            return 'utf-8'
    def process_record(self, record):
        if self.is_response_record(record):
            if record.rec_type == 'response':
                if any(header[0] == 'Content-Type' for header in record.http_headers.headers) and 'text/html' in record.http_headers['Content-Type']:
                    # Extract HTML content
                    html_content = record.content_stream().read()
                    detected_encoding = ""
                    try:
                        decoded_content = html_content.decode('utf-8')
                        detected_encoding = 'utf-8'
                    except UnicodeDecodeError:
                        # If decoding with UTF-8 fails, try ISO-8859-1
                        try:
                            decoded_content = html_content.decode('ISO-8859-1')
                            detected_encoding = 'ISO-8859-1'
                        except UnicodeDecodeError:
                            decoded_content = html_content.decode(self.detect_encoding(html_content), errors='replace')
                            detected_encoding = self.detect_encoding(html_content)
                    
                    try:
                        soup = BeautifulSoup(decoded_content, 'lxml', from_encoding=detected_encoding)
                    except Exception as e:
                        print(f"Failed with encoding {detected_encoding}: {e}")
                        return
                    
                    meta_tags = soup.find_all('meta')
                    interesting_meta_tag_names = ['robots','author','copyright','tdm-reservation','tdm-policy']
                    for tag in meta_tags:
                        if tag.get('name') in interesting_meta_tag_names:
                            tag_name = tag.get('name')
                            content = tag.get('content')
                            key = (tag_name, str(content).lower())
                            yield key, 1

        else:
            # warcinfo, request, non-WAT metadata records
            pass


if __name__ == "__main__":
    job = HtmlMetatagCount()
    job.run()
